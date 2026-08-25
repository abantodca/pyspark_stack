"""MEDALLION E2E — Pipeline Daily Sales."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "daily_sales"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.02
SUPPORTED_CURRENCIES = ["PEN", "USD"]
SOURCE_ENV_VAR = "DAILY_SALES_SOURCE_URI"
SAMPLE_SCHEMA = (
    "order_id string, channel string, sku string, sold_at string, "
    "quantity bigint, unit_price double, currency string"
)
SAMPLE_SALES = [
    ("O-1001", "store", "SKU-1", "2026-01-05T14:10:00Z", 2, 59.90, "PEN"),
    ("O-1002", "web", "SKU-2", "2026-01-05T15:20:00Z", 1, 249.00, "PEN"),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza las líneas de POS y ecommerce tal como se exportan."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.option("header", True).csv(uri)
            if uri
            else spark.createDataFrame(SAMPLE_SALES, SAMPLE_SCHEMA)
        )
        columns = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("pos_and_ecommerce"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *columns), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Tipifica importes, rechaza líneas invendibles y deduplica por pedido y SKU."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("sold_at", F.to_timestamp("sold_at"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
            .withColumn("channel", F.lower(F.trim("channel")))
            .withColumn("currency", F.upper(F.trim("currency")))
            .withColumn(
                "line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
            )
        )
        reason = (
            F.when(F.col("order_id").isNull(), "missing_order_id")
            .when(F.col("sku").isNull(), "missing_sku")
            .when(F.col("sold_at").isNull(), "invalid_sold_at")
            .when(
                (F.col("quantity") <= 0) | (F.col("unit_price") < 0), "invalid_amount"
            )
            .when(
                ~F.col("currency").isin(SUPPORTED_CURRENCIES), "unsupported_currency"
            )
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("order_id", "sku").orderBy(
            F.col("sold_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
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
    """Publica ingresos, unidades y ticket medio por canal."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = spark.read.parquet(RUNTIME.path("silver", run_date)).groupBy(
            F.to_date("sold_at").alias("sale_date"), "channel", "currency"
        ).agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("line_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("line_amount") / F.countDistinct("order_id"), 2).alias(
                "average_order_value"
            ),
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_daily_sales",
    description="POS and ecommerce sales to daily channel KPIs",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "commercial-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "sales", "finance"],
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
