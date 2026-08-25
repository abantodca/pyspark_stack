"""MEDALLION E2E — Pipeline Supplier Performance."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "supplier_performance"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.03
UNIT_COLUMNS = ("ordered_units", "accepted_units", "delay_days")
SOURCE_ENV_VAR = "SUPPLIER_PERFORMANCE_SOURCE_URI"
SAMPLE_SCHEMA = (
    "delivery_id string, supplier_id string, delivered_at string, "
    "ordered_units bigint, accepted_units bigint, delay_days bigint"
)
SAMPLE_DELIVERIES = [
    ("D-1001", "SUP-01", "2026-01-05T08:00:00Z", 100, 98, 2),
    ("D-1002", "SUP-02", "2026-01-05T08:15:00Z", 80, 70, 4),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza las recepciones del ERP tal como se exportan."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.option("header", True).csv(uri)
            if uri
            else spark.createDataFrame(SAMPLE_DELIVERIES, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("procurement_erp"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Rechaza recepciones descuadradas y deriva fill rate y puntualidad."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = spark.read.parquet(RUNTIME.path("bronze", run_date)).withColumn(
            "delivered_at", F.to_timestamp("delivered_at")
        )
        for column in UNIT_COLUMNS:
            frame = frame.withColumn(column, F.col(column).cast("int"))
        reason = (
            F.when(
                F.col("delivery_id").isNull() | F.col("supplier_id").isNull(),
                "missing_delivery_key",
            )
            .when(F.col("delivered_at").isNull(), "invalid_delivered_at")
            .when(
                (F.col("ordered_units") <= 0)
                | (F.col("accepted_units") < 0)
                | (F.col("accepted_units") > F.col("ordered_units")),
                "invalid_unit_balance",
            )
            .when(F.col("delay_days") < 0, "negative_delay")
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("delivery_id").orderBy(
            F.col("delivered_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn(
                "fill_rate",
                F.round(F.col("accepted_units") / F.col("ordered_units"), 4),
            )
            .withColumn("on_time", F.col("delay_days") == 0)
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
    """Publica el scorecard de proveedores usado en las revisiones de compras."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy("supplier_id")
            .agg(
                F.countDistinct("delivery_id").alias("deliveries"),
                F.round(F.avg("fill_rate"), 4).alias("avg_fill_rate"),
                F.round(F.avg(F.col("on_time").cast("double")), 4).alias(
                    "on_time_delivery_rate"
                ),
                F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
            )
            .withColumn("scorecard_date", F.lit(run_date).cast("date"))
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_supplier_performance",
    description="Procurement deliveries to supplier scorecards",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 6 * * 1",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "procurement",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "supplier", "procurement"],
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
