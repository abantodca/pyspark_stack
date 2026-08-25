"""MEDALLION E2E — Pipeline Customer Churn Features."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "customer_churn_features"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.02
USAGE_WINDOW_DAYS = 30
OPEN_TICKET_STATUSES = ["open", "pending"]
HIGH_PRIORITIES = ["high", "urgent"]


def bronze_ingest(run_date: str) -> None:
    """Ingest CRM, subscription, product-usage and support sources independently."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        customers_uri = os.getenv("CHURN_CUSTOMERS_SOURCE_URI")
        subscriptions_uri = os.getenv("CHURN_SUBSCRIPTIONS_SOURCE_URI")
        usage_uri = os.getenv("CHURN_USAGE_SOURCE_URI")
        tickets_uri = os.getenv("CHURN_TICKETS_SOURCE_URI")
        # Sin URIs configuradas corre con estas filas: C-1002 casi no usa el
        # producto y tiene un ticket urgente abierto, la señal que busca el score.
        customers = (
            spark.read.json(customers_uri)
            if customers_uri
            else spark.createDataFrame(
                [
                    ("C-1001", "enterprise", "ana@company.pe", f"{run_date}T08:00:00Z"),
                    ("C-1002", "smb", "luis@business.pe", f"{run_date}T08:05:00Z"),
                ],
                "customer_id string, segment string, email string, updated_at string",
            )
        )
        subscriptions = (
            spark.read.json(subscriptions_uri)
            if subscriptions_uri
            else spark.createDataFrame(
                [
                    ("S-1001", "C-1001", "pro", "active", f"{run_date}T08:10:00Z", 499.0),
                    ("S-1002", "C-1002", "basic", "past_due", f"{run_date}T08:12:00Z", 99.0),
                ],
                "subscription_id string, customer_id string, plan string, "
                "status string, effective_at string, mrr double",
            )
        )
        usage = (
            spark.read.option("header", True).csv(usage_uri)
            if usage_uri
            else spark.createDataFrame(
                [
                    ("U-1001", "C-1001", run_date, 18, 7),
                    ("U-1002", "C-1002", run_date, 1, 1),
                ],
                "usage_id string, customer_id string, activity_date string, "
                "sessions bigint, active_users bigint",
            )
        )
        tickets = (
            spark.read.json(tickets_uri)
            if tickets_uri
            else spark.createDataFrame(
                [
                    ("T-1001", "C-1001", "resolved", "normal", f"{run_date}T09:00:00Z"),
                    ("T-1002", "C-1002", "open", "urgent", f"{run_date}T09:10:00Z"),
                ],
                "ticket_id string, customer_id string, status string, "
                "priority string, opened_at string",
            )
        )

        for name, source in {
            "customers": customers,
            "subscriptions": subscriptions,
            "usage": usage,
            "tickets": tickets,
        }.items():
            values = [
                F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                for c in sorted(source.columns)
            ]
            bronze = (
                source.withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_dataset", F.lit(name))
                .withColumn("_contract_version", F.lit("1.0.0"))
                .withColumn("_record_hash", F.sha2(F.concat_ws("||", *values), 256))
            )
            RUNTIME.write(bronze, "bronze", run_date, name)
    finally:
        spark.stop()


def silver_feature_engineering(run_date: str) -> None:
    """Conform sources and build one leakage-safe feature row per customer."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        as_of = F.lit(run_date).cast("date")
        customers = (
            spark.read.parquet(RUNTIME.path("bronze", run_date, "customers"))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("segment", F.lower(F.trim("segment")))
            # El correo en claro se queda en Bronze; aguas abajo solo viaja el hash.
            .withColumn("email_hash", F.sha2(F.lower(F.trim("email")), 256))
            .select("customer_id", "segment", "email_hash", "updated_at")
            .cache()
        )
        subscriptions = (
            spark.read.parquet(RUNTIME.path("bronze", run_date, "subscriptions"))
            .withColumn("effective_at", F.to_timestamp("effective_at"))
            .withColumn("mrr", F.col("mrr").cast("decimal(18,2)"))
            .withColumn("status", F.lower(F.trim("status")))
        )
        latest_window = Window.partitionBy("customer_id").orderBy(
            F.col("effective_at").desc()
        )
        latest_subscription = (
            subscriptions.withColumn("_rn", F.row_number().over(latest_window))
            .filter("_rn = 1")
            .select(
                "subscription_id",
                "customer_id",
                "plan",
                "status",
                "effective_at",
                "mrr",
            )
        )
        usage_30d = (
            spark.read.parquet(RUNTIME.path("bronze", run_date, "usage"))
            .withColumn("activity_date", F.to_date("activity_date"))
            .withColumn("sessions", F.col("sessions").cast("long"))
            .withColumn("active_users", F.col("active_users").cast("long"))
            .filter(F.col("activity_date") >= F.date_sub(as_of, USAGE_WINDOW_DAYS - 1))
            .groupBy("customer_id")
            .agg(
                F.sum("sessions").alias("sessions_30d"),
                F.max("activity_date").alias("last_activity_date"),
                F.max("active_users").alias("peak_active_users_30d"),
            )
        )
        ticket_features = (
            spark.read.parquet(RUNTIME.path("bronze", run_date, "tickets"))
            .withColumn("opened_at", F.to_timestamp("opened_at"))
            .groupBy("customer_id")
            .agg(
                F.sum(F.col("status").isin(OPEN_TICKET_STATUSES).cast("int")).alias(
                    "open_tickets"
                ),
                F.sum(F.col("priority").isin(HIGH_PRIORITIES).cast("int")).alias(
                    "high_priority_tickets"
                ),
            )
        )
        features = (
            customers.join(latest_subscription, "customer_id", "left")
            .join(usage_30d, "customer_id", "left")
            .join(ticket_features, "customer_id", "left")
            .fillna(
                {
                    "sessions_30d": 0,
                    "peak_active_users_30d": 0,
                    "open_tickets": 0,
                    "high_priority_tickets": 0,
                }
            )
            .withColumn(
                "days_since_activity", F.datediff(as_of, "last_activity_date")
            )
            .withColumn("feature_as_of_date", as_of)
        )
        invalid = features.filter(
            F.col("customer_id").isNull()
            | F.col("subscription_id").isNull()
            | F.col("mrr").isNull()
            | (F.col("mrr") < 0)
        )
        valid = features.join(
            invalid.select("customer_id"), "customer_id", "left_anti"
        ).cache()
        received, rejected, published = (
            customers.count(),
            invalid.count(),
            valid.count(),
        )
        quarantined = invalid.withColumn(
            "_reject_reason", F.lit("invalid_customer_or_subscription_contract")
        )
        RUNTIME.write(quarantined, "quarantine", run_date)
        RUNTIME.enforce_quality(
            spark,
            run_date,
            received=received,
            rejected=rejected,
            published=published,
            max_rejected_ratio=MAX_REJECT_RATIO,
        )
        RUNTIME.write(valid, "silver", run_date)
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Publish customer-level health scores and an operational risk summary."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        # Peso y causa van juntos: la banda en la que cae un cliente es explicable.
        rules = (
            (F.col("status") == "past_due", 0.40),
            (F.col("sessions_30d") < 3, 0.25),
            (F.col("days_since_activity") > 14, 0.20),
            (F.col("high_priority_tickets") > 0, 0.15),
        )
        weighted = F.lit(0.0)
        for condition, weight in rules:
            weighted = weighted + F.when(condition, weight).otherwise(0.0)

        scored = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .withColumn("churn_risk_score", F.round(F.least(F.lit(1.0), weighted), 2))
            .withColumn(
                "risk_band",
                F.when(F.col("churn_risk_score") >= 0.70, "critical")
                .when(F.col("churn_risk_score") >= 0.40, "high")
                .when(F.col("churn_risk_score") >= 0.20, "medium")
                .otherwise("low"),
            )
        )
        RUNTIME.write(scored, "gold", run_date, "customer_scores")
        risk_summary = scored.groupBy("segment", "plan", "risk_band").agg(
            F.countDistinct("customer_id").alias("customers"),
            F.round(F.sum("mrr"), 2).alias("mrr_at_risk"),
            F.round(F.avg("churn_risk_score"), 3).alias("avg_risk_score"),
        )
        RUNTIME.write(risk_summary, "gold", run_date, "risk_summary")
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_customer_churn_features",
    description="Multi-source customer health features and explainable churn-risk scoring",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "customer-intelligence",
        "retries": 2,
        "retry_delay": timedelta(minutes=4),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "churn", "feature-engineering", "pii"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_feature_engineering",
        python_callable=silver_feature_engineering,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
