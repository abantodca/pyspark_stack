"""MEDALLION E2E — Pipeline Inventory Snapshot."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "inventory_snapshot"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.02
QUANTITY_COLUMNS = ("on_hand_qty", "reserved_qty", "reorder_point")
SOURCE_ENV_VAR = "INVENTORY_SNAPSHOT_SOURCE_URI"
SAMPLE_SCHEMA = (
    "warehouse_id string, sku string, snapshot_at string, "
    "on_hand_qty bigint, reserved_qty bigint, reorder_point bigint"
)
SAMPLE_SNAPSHOTS = [
    ("LIM-01", "SKU-1", "2026-01-05T02:00:00Z", 25, 5, 10),
    ("CAL-01", "SKU-2", "2026-01-05T02:00:00Z", 8, 2, 12),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza el snapshot del WMS tal como se exporta."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.option("header", True).csv(uri)
            if uri
            else spark.createDataFrame(SAMPLE_SNAPSHOTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("warehouse_management_system"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Rechaza balances imposibles y conserva el snapshot más reciente por SKU."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = spark.read.parquet(RUNTIME.path("bronze", run_date)).withColumn(
            "snapshot_at", F.to_timestamp("snapshot_at")
        )
        for column in QUANTITY_COLUMNS:
            frame = frame.withColumn(column, F.col(column).cast("long"))
        reason = (
            F.when(
                F.col("warehouse_id").isNull() | F.col("sku").isNull(),
                "missing_inventory_key",
            )
            .when(F.col("snapshot_at").isNull(), "invalid_snapshot_at")
            .when(
                (F.col("on_hand_qty") < 0)
                | (F.col("reserved_qty") < 0)
                | (F.col("reserved_qty") > F.col("on_hand_qty")),
                "invalid_stock_balance",
            )
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("warehouse_id", "sku").orderBy(
            F.col("snapshot_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("available_qty", F.col("on_hand_qty") - F.col("reserved_qty"))
            .withColumn(
                "needs_replenishment", F.col("available_qty") <= F.col("reorder_point")
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
    """Publica cobertura de stock y presión de reposición por almacén."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy("warehouse_id")
            .agg(
                F.countDistinct("sku").alias("sku_count"),
                F.sum("on_hand_qty").alias("on_hand_units"),
                F.sum("available_qty").alias("available_units"),
                F.sum(F.col("needs_replenishment").cast("int")).alias(
                    "skus_to_replenish"
                ),
            )
            .withColumn("snapshot_date", F.lit(run_date).cast("date"))
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_inventory_snapshot",
    description="Warehouse stock snapshots to replenishment KPIs",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "supply-chain",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "inventory", "supply-chain"],
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
