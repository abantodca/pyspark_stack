"""MEDALLION E2E — Pipeline Web Events."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "web_events"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.05
KNOWN_EVENTS = ["page_view", "add_to_cart", "checkout", "purchase"]
SOURCE_ENV_VAR = "WEB_EVENTS_SOURCE_URI"
SAMPLE_SCHEMA = (
    "event_id string, session_id string, user_id string, event_name string, "
    "page_path string, event_at string, device_type string"
)
SAMPLE_EVENTS = [
    ("E-1001", "S-101", "U-101", "page_view", "/products/sku-1", "2026-01-05T12:00:00Z", "desktop"),
    ("E-1002", "S-101", "U-101", "add_to_cart", "/products/sku-1", "2026-01-05T12:02:00Z", "desktop"),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza los eventos del SDK tal como llegan y agrega linaje."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(uri)
            if uri
            else spark.createDataFrame(SAMPLE_EVENTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("web_tracking_sdk"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Descarta eventos fuera de la taxonomía y deduplica reenvíos."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("event_at", F.to_timestamp("event_at"))
            .withColumn("event_name", F.lower(F.trim("event_name")))
            .withColumn("device_type", F.lower(F.trim("device_type")))
        )
        reason = (
            F.when(
                F.col("event_id").isNull() | F.col("session_id").isNull(),
                "missing_event_key",
            )
            .when(F.col("event_at").isNull(), "invalid_event_at")
            .when(~F.col("event_name").isin(KNOWN_EVENTS), "unknown_event")
            .when(~F.col("page_path").startswith("/"), "invalid_page_path")
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("event_id").orderBy(
            F.col("event_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("event_date", F.to_date("event_at"))
            .withColumn("event_hour", F.hour("event_at"))
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
    """Publica tráfico y engagement por hora, dispositivo y evento."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = spark.read.parquet(RUNTIME.path("silver", run_date)).groupBy(
            "event_date", "event_hour", "device_type", "event_name"
        ).agg(
            F.countDistinct("event_id").alias("events"),
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("user_id").alias("identified_users"),
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_web_events",
    description="Web tracking events to product analytics aggregates",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "product-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "web", "events"],
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
