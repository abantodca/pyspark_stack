"""MEDALLION E2E — Pipeline Product Catalog."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "product_catalog"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.02
VALID_STATUSES = ["draft", "active", "discontinued"]
SOURCE_ENV_VAR = "PRODUCT_CATALOG_SOURCE_URI"
SAMPLE_SCHEMA = (
    "sku string, product_name string, category string, status string, "
    "updated_at string, list_price double, currency string"
)
SAMPLE_PRODUCTS = [
    ("SKU-1", "Wireless Mouse", "accessories", "active", "2026-01-05T01:00:00Z", 59.90, "PEN"),
    ("SKU-2", "Mechanical Keyboard", "hardware", "active", "2026-01-05T01:00:00Z", 249.00, "PEN"),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza el catálogo tal como lo exporta el PIM."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(uri)
            if uri
            else spark.createDataFrame(SAMPLE_PRODUCTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("product_information_management"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Tipifica precios, valida el contrato y conserva la última versión del SKU."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("list_price", F.col("list_price").cast("decimal(18,2)"))
            .withColumn("category", F.lower(F.trim("category")))
            .withColumn("status", F.lower(F.trim("status")))
            .withColumn("currency", F.upper(F.trim("currency")))
        )
        reason = (
            F.when(F.col("sku").isNull() | (F.trim("sku") == ""), "missing_sku")
            .when(F.col("product_name").isNull(), "missing_product_name")
            .when(F.col("updated_at").isNull(), "invalid_updated_at")
            .when(F.col("list_price") < 0, "negative_price")
            .when(~F.col("status").isin(VALID_STATUSES), "unknown_status")
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("sku").orderBy(
            F.col("updated_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("is_sellable", F.col("status") == "active")
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
    """Publica amplitud del surtido y dispersión de precios por categoría."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy("category", "currency")
            .agg(
                F.countDistinct("sku").alias("catalog_skus"),
                F.sum(F.col("is_sellable").cast("int")).alias("sellable_skus"),
                F.round(F.avg("list_price"), 2).alias("avg_list_price"),
                F.round(F.min("list_price"), 2).alias("min_list_price"),
                F.round(F.max("list_price"), 2).alias("max_list_price"),
            )
            .withColumn("as_of_date", F.lit(run_date).cast("date"))
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_product_catalog",
    description="PIM catalogue to governed sellable-product metrics",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "product-data",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "catalog", "product"],
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
