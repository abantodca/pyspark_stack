"""MEDALLION E2E — Pipeline AML Transaction Monitoring."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "aml_transaction_monitoring"
RUNTIME = MedallionRuntime(PROJECT)

# Una transacción que no llega a Silver es un punto ciego del monitoreo.
MAX_REJECT_RATIO = 0.005
SUPPORTED_CURRENCIES = ["USD", "PEN", "EUR"]
VELOCITY_WINDOW_SECONDS = 24 * 60 * 60
ALERT_THRESHOLD = 0.60
CRITICAL_THRESHOLD = 0.85
STALE_KYC_DAYS = 365


def bronze_ingest(run_date: str) -> None:
    """Land payments, KYC profiles and jurisdiction watchlists with source lineage."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        transactions_uri = os.getenv("AML_TRANSACTIONS_SOURCE_URI")
        customers_uri = os.getenv("AML_CUSTOMERS_SOURCE_URI")
        watchlist_uri = os.getenv("AML_WATCHLIST_SOURCE_URI")
        # Sin URIs configuradas corre con estas filas: dos transferencias a una
        # jurisdicción vigilada, justo por debajo del umbral de reporte.
        transactions = (
            spark.read.json(transactions_uri)
            if transactions_uri
            else spark.createDataFrame(
                [
                    ("TX-1001", "C-1001", "A-PE-01", "A-US-77", "PE", "US", f"{run_date}T10:00:00Z", 900.0, "USD", "wire"),
                    ("TX-1002", "C-1002", "A-PE-02", "A-XR-99", "PE", "XR", f"{run_date}T10:02:00Z", 12500.0, "USD", "wire"),
                    ("TX-1003", "C-1002", "A-PE-02", "A-XR-98", "PE", "XR", f"{run_date}T10:04:00Z", 9800.0, "USD", "wire"),
                ],
                "transaction_id string, customer_id string, origin_account string, "
                "beneficiary_account string, origin_country string, "
                "beneficiary_country string, event_at string, amount double, "
                "currency string, channel string",
            )
        )
        customers = (
            spark.read.json(customers_uri)
            if customers_uri
            else spark.createDataFrame(
                [
                    ("C-1001", "low", "verified", run_date, "PE"),
                    ("C-1002", "high", "verified", run_date, "PE"),
                ],
                "customer_id string, kyc_risk string, kyc_status string, "
                "kyc_review_date string, residence_country string",
            )
        )
        watchlist = (
            spark.read.option("header", True).csv(watchlist_uri)
            if watchlist_uri
            else spark.createDataFrame(
                [("XR", "restricted", 0.50), ("XZ", "high_risk", 0.30)],
                "country_code string, risk_category string, jurisdiction_weight double",
            )
        )
        for name, source in {
            "transactions": transactions,
            "customers": customers,
            "watchlist": watchlist,
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


def silver_risk_features(run_date: str) -> None:
    """Validate contracts and compute 24-hour velocity plus KYC/jurisdiction features."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        transactions = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "transactions")
        ).select(
            "transaction_id",
            "customer_id",
            "origin_account",
            "beneficiary_account",
            F.upper("origin_country").alias("origin_country"),
            F.upper("beneficiary_country").alias("beneficiary_country"),
            F.to_timestamp("event_at").alias("event_at"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.upper("currency").alias("currency"),
            F.lower("channel").alias("channel"),
            "_ingested_at",
        ).cache()
        customers = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "customers")
        ).select(
            "customer_id",
            F.lower("kyc_risk").alias("kyc_risk"),
            F.lower("kyc_status").alias("kyc_status"),
            F.to_date("kyc_review_date").alias("kyc_review_date"),
            F.upper("residence_country").alias("residence_country"),
        )
        watchlist = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "watchlist")
        ).select(
            F.upper("country_code").alias("watchlist_country"),
            "risk_category",
            F.col("jurisdiction_weight").cast("double").alias("jurisdiction_weight"),
        )
        invalid = transactions.filter(
            F.col("transaction_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("event_at").isNull()
            | F.col("amount").isNull()
            | (F.col("amount") <= 0)
            | ~F.col("currency").isin(SUPPORTED_CURRENCIES)
        )
        clean = transactions.join(
            invalid.select("transaction_id"), "transaction_id", "left_anti"
        )
        dedupe = Window.partitionBy("transaction_id").orderBy(
            F.col("event_at").desc(), F.col("_ingested_at").desc()
        )
        clean = (
            clean.withColumn("_rn", F.row_number().over(dedupe))
            .filter("_rn = 1")
            .drop("_rn")
        )
        # La misma ventana de 24 h alimenta el conteo y el importe acumulado.
        last_24h = (
            Window.partitionBy("customer_id")
            .orderBy("_event_epoch")
            .rangeBetween(-VELOCITY_WINDOW_SECONDS, 0)
        )
        velocity = (
            clean.withColumn("_event_epoch", F.col("event_at").cast("long"))
            .withColumn("transactions_24h", F.count("*").over(last_24h))
            .withColumn("amount_24h", F.sum("amount").over(last_24h))
            .drop("_event_epoch")
        )
        features = (
            velocity.join(customers, "customer_id", "left")
            .join(
                watchlist,
                velocity.beneficiary_country == watchlist.watchlist_country,
                "left",
            )
            .drop("watchlist_country")
            .fillna({"jurisdiction_weight": 0.0, "risk_category": "standard"})
            .withColumn(
                "kyc_age_days",
                F.datediff(F.lit(run_date).cast("date"), "kyc_review_date"),
            )
        )
        # Un pago sin perfil KYC no se puede screenear: es excepción, no dato válido.
        missing_kyc = features.filter(F.col("kyc_status").isNull())
        valid = features.filter(F.col("kyc_status").isNotNull()).cache()
        rejected = invalid.count() + missing_kyc.count()
        received, published = transactions.count(), valid.count()
        invalid_transactions = invalid.withColumn(
            "_reject_reason", F.lit("invalid_transaction_contract")
        )
        missing_kyc_records = missing_kyc.withColumn(
            "_reject_reason", F.lit("customer_without_kyc")
        )
        RUNTIME.write(invalid_transactions, "quarantine", run_date, "transactions")
        RUNTIME.write(missing_kyc_records, "quarantine", run_date, "missing_kyc")
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
    """Apply explainable rules and publish case-level alerts plus control totals."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        features = spark.read.parquet(RUNTIME.path("silver", run_date))
        is_alert = F.col("risk_score") >= ALERT_THRESHOLD
        # Cada regla lleva junto el peso que suma y el nombre que reporta: el
        # score nunca es mayor que lo que la alerta puede explicar.
        rules = (
            (F.col("amount") >= 10000, "large_transaction", F.lit(0.30)),
            (F.col("amount_24h") >= 20000, "high_24h_value", F.lit(0.25)),
            (F.col("transactions_24h") >= 3, "high_velocity", F.lit(0.15)),
            (F.col("kyc_risk") == "high", "high_kyc_risk", F.lit(0.20)),
            (F.col("kyc_age_days") > STALE_KYC_DAYS, "stale_kyc_review", F.lit(0.10)),
            (
                F.col("jurisdiction_weight") > 0,
                "watched_jurisdiction",
                F.col("jurisdiction_weight"),
            ),
        )
        weighted = F.lit(0.0)
        for condition, _name, weight in rules:
            weighted = weighted + F.when(condition, weight).otherwise(F.lit(0.0))

        scored = features.withColumn(
            "risk_score", F.round(F.least(F.lit(1.0), weighted), 2)
        ).withColumn(
            "triggered_rules",
            F.array_compact(
                F.array(*[F.when(condition, name) for condition, name, _ in rules])
            ),
        )
        alerts = scored.filter(is_alert).withColumn(
            "alert_priority",
            F.when(F.col("risk_score") >= CRITICAL_THRESHOLD, "critical").otherwise(
                "high"
            ),
        )
        RUNTIME.write(alerts, "gold", run_date, "alerts")

        control_summary = scored.groupBy(
            F.to_date("event_at").alias("business_date"), "channel", "currency"
        ).agg(
            F.countDistinct("transaction_id").alias("transactions_screened"),
            F.sum(is_alert.cast("int")).alias("alerts_created"),
            F.round(F.sum("amount"), 2).alias("screened_amount"),
            F.round(F.sum(F.when(is_alert, F.col("amount")).otherwise(0)), 2).alias(
                "alerted_amount"
            ),
        )
        RUNTIME.write(control_summary, "gold", run_date, "control_summary")
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_aml_transaction_monitoring",
    description="Transaction, KYC and watchlist enrichment with explainable AML alerts",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "financial-crime-data",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["medallion", "aml", "risk", "financial-crime"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_risk_features",
        python_callable=silver_risk_features,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
