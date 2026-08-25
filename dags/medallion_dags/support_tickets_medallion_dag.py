"""MEDALLION E2E — Pipeline Support Tickets."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "support_tickets"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.03
SECONDS_PER_HOUR = 3600
# Horas para resolver según prioridad declarada; "low" es además el respaldo.
SLA_TARGET_HOURS = {"urgent": 2, "high": 4, "normal": 12, "low": 24}
VALID_STATUSES = ["open", "pending", "resolved", "closed"]
OPEN_STATUSES = ["open", "pending"]
SOURCE_ENV_VAR = "SUPPORT_TICKETS_SOURCE_URI"
SAMPLE_SCHEMA = (
    "ticket_id string, customer_id string, category string, priority string, "
    "opened_at string, resolved_at string, status string"
)
SAMPLE_TICKETS = [
    ("TK-1001", "C-101", "billing", "high", "2026-01-05T08:00:00Z", "2026-01-05T10:00:00Z", "resolved"),
    ("TK-1002", "C-102", "technical", "normal", "2026-01-05T09:00:00Z", None, "open"),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza los casos de soporte tal como los exporta la plataforma."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(uri)
            if uri
            else spark.createDataFrame(SAMPLE_TICKETS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("customer_support_platform"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Valida el contrato del caso y mide la resolución contra su SLA."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("opened_at", F.to_timestamp("opened_at"))
            .withColumn("resolved_at", F.to_timestamp("resolved_at"))
            .withColumn("category", F.lower(F.trim("category")))
            .withColumn("priority", F.lower(F.trim("priority")))
            .withColumn("status", F.lower(F.trim("status")))
        )
        reason = (
            F.when(
                F.col("ticket_id").isNull() | F.col("customer_id").isNull(),
                "missing_ticket_key",
            )
            .when(F.col("opened_at").isNull(), "invalid_opened_at")
            .when(~F.col("priority").isin(list(SLA_TARGET_HOURS)), "unknown_priority")
            .when(~F.col("status").isin(VALID_STATUSES), "unknown_status")
            .when(F.col("resolved_at") < F.col("opened_at"), "resolution_before_open")
        )
        sla_target = F.lit(SLA_TARGET_HOURS["low"])
        for priority, hours in SLA_TARGET_HOURS.items():
            sla_target = F.when(F.col("priority") == priority, hours).otherwise(
                sla_target
            )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        # Un caso sin resolver se ordena por el momento en que se abrió.
        window = Window.partitionBy("ticket_id").orderBy(
            F.coalesce(F.col("resolved_at"), F.col("opened_at")).desc(),
            F.col("_ingested_at").desc(),
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn(
                "resolution_hours",
                F.when(
                    F.col("resolved_at").isNotNull(),
                    F.round(
                        (
                            F.unix_timestamp("resolved_at")
                            - F.unix_timestamp("opened_at")
                        )
                        / SECONDS_PER_HOUR,
                        2,
                    ),
                ),
            )
            .withColumn("sla_target_hours", sla_target)
            .withColumn(
                "within_sla",
                F.when(
                    F.col("resolved_at").isNotNull(),
                    F.col("resolution_hours") <= F.col("sla_target_hours"),
                ),
            )
            .cache()
        )
        received, invalid, published = (
            checked.count(),
            rejected.count(),
            silver.count(),
        )
        RUNTIME.write(rejected, "quarantine", run_date)
        RUNTIME.enforce_quality(
            spark,
            run_date,
            received=received,
            rejected=invalid,
            published=published,
            max_rejected_ratio=MAX_REJECT_RATIO,
        )
        RUNTIME.write(silver, "silver", run_date)
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Publica backlog y cumplimiento de SLA por categoría y prioridad."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = spark.read.parquet(RUNTIME.path("silver", run_date)).groupBy(
            F.to_date("opened_at").alias("opened_date"), "category", "priority"
        ).agg(
            F.countDistinct("ticket_id").alias("tickets"),
            F.sum(F.col("status").isin(OPEN_STATUSES).cast("int")).alias(
                "open_backlog"
            ),
            F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
            F.round(F.avg(F.col("within_sla").cast("double")), 4).alias(
                "sla_compliance_rate"
            ),
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_support_tickets",
    description="Support cases to SLA and backlog service metrics",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "customer-operations",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "support", "sla"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_conform",
        python_callable=silver_conform,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
