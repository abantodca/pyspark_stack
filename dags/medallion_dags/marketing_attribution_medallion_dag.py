"""MEDALLION E2E — Pipeline Marketing Attribution."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "marketing_attribution"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.03
SOURCE_ENV_VAR = "MARKETING_ATTRIBUTION_SOURCE_URI"
SAMPLE_SCHEMA = (
    "touchpoint_id string, customer_id string, channel string, campaign string, "
    "touch_at string, attribution_weight double, conversion_revenue double"
)
SAMPLE_TOUCHPOINTS = [
    ("T-1001", "C-101", "email", "welcome-q1", "2026-01-05T09:00:00Z", 0.40, 250.00),
    ("T-1002", "C-102", "paid_search", "brand-sem", "2026-01-05T09:15:00Z", 1.00, 120.00),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza los touchpoints tal como los emite la plataforma de medición."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(uri)
            if uri
            else spark.createDataFrame(SAMPLE_TOUCHPOINTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("marketing_measurement_platform"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Valida los pesos de atribución y reparte el ingreso entre touchpoints."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("touch_at", F.to_timestamp("touch_at"))
            .withColumn(
                "attribution_weight", F.col("attribution_weight").cast("double")
            )
            .withColumn(
                "conversion_revenue", F.col("conversion_revenue").cast("decimal(18,2)")
            )
            .withColumn("channel", F.lower(F.trim("channel")))
            .withColumn("campaign", F.lower(F.trim("campaign")))
        )
        reason = (
            F.when(
                F.col("touchpoint_id").isNull() | F.col("customer_id").isNull(),
                "missing_touchpoint_key",
            )
            .when(F.col("touch_at").isNull(), "invalid_touch_at")
            .when(
                ~F.col("attribution_weight").between(0.0, 1.0),
                "invalid_attribution_weight",
            )
            .when(F.col("conversion_revenue") < 0, "negative_revenue")
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("touchpoint_id").orderBy(
            F.col("touch_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn(
                "attributed_revenue",
                F.round(F.col("conversion_revenue") * F.col("attribution_weight"), 2),
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
    """Publica ingreso atribuido y alcance por canal y campaña."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy(
                F.to_date("touch_at").alias("attribution_date"), "channel", "campaign"
            )
            .agg(
                F.countDistinct("customer_id").alias("reached_customers"),
                F.countDistinct("touchpoint_id").alias("touchpoints"),
                F.round(F.sum("attributed_revenue"), 2).alias("attributed_revenue"),
                F.round(F.avg("attribution_weight"), 4).alias("avg_weight"),
            )
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_marketing_attribution",
    description="Marketing touchpoints to campaign attribution KPIs",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "growth-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "marketing", "attribution"],
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
