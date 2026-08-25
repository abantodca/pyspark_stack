"""MEDALLION E2E — Pipeline Payment Reconciliation."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "payment_reconciliation"
RUNTIME = MedallionRuntime(PROJECT)

# Dinero: medio punto porcentual de rechazos ya detiene el lote.
MAX_REJECT_RATIO = 0.005
# El ruido de redondeo por debajo de un centavo no es un descuadre.
MATCH_TOLERANCE = 0.01
VALID_STATUSES = ["approved", "settled", "declined", "refunded"]
SOURCE_ENV_VAR = "PAYMENT_RECONCILIATION_SOURCE_URI"
SAMPLE_SCHEMA = (
    "payment_id string, order_id string, provider string, processed_at string, "
    "status string, order_amount double, settled_amount double, currency string"
)
SAMPLE_PAYMENTS = [
    ("P-1001", "O-1001", "gateway-a", "2026-01-05T20:01:00Z", "approved", 119.80, 119.80, "PEN"),
    ("P-1002", "O-1002", "gateway-b", "2026-01-05T20:02:00Z", "settled", 249.00, 249.00, "PEN"),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza cada export del gateway tal como se recibe."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.option("header", True).csv(uri)
            if uri
            else spark.createDataFrame(SAMPLE_PAYMENTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("payment_gateway_exports"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Valida el contrato del pago y clasifica cada diferencia de liquidación."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("processed_at", F.to_timestamp("processed_at"))
            .withColumn("order_amount", F.col("order_amount").cast("decimal(18,2)"))
            .withColumn("settled_amount", F.col("settled_amount").cast("decimal(18,2)"))
            .withColumn("status", F.lower(F.trim("status")))
            .withColumn("currency", F.upper(F.trim("currency")))
        )
        reason = (
            F.when(
                F.col("payment_id").isNull() | F.col("order_id").isNull(),
                "missing_payment_key",
            )
            .when(F.col("processed_at").isNull(), "invalid_processed_at")
            .when(~F.col("status").isin(VALID_STATUSES), "unknown_status")
            .when(
                (F.col("order_amount") < 0) | (F.col("settled_amount") < 0),
                "negative_amount",
            )
        )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("payment_id").orderBy(
            F.col("processed_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn(
                "variance_amount",
                F.round(F.col("order_amount") - F.col("settled_amount"), 2),
            )
            .withColumn(
                "reconciliation_status",
                F.when(
                    F.abs(F.col("variance_amount")) <= F.lit(MATCH_TOLERANCE), "matched"
                ).otherwise("mismatch"),
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
    """Publica lo liquidado frente a lo esperado por proveedor y día."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy(
                F.to_date("processed_at").alias("business_date"),
                "provider",
                "currency",
                "reconciliation_status",
            )
            .agg(
                F.countDistinct("payment_id").alias("payments"),
                F.round(F.sum("order_amount"), 2).alias("expected_amount"),
                F.round(F.sum("settled_amount"), 2).alias("settled_amount"),
                F.round(F.sum("variance_amount"), 2).alias("variance_amount"),
            )
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_payment_reconciliation",
    description="Gateway settlements to finance reconciliation controls",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "finance-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "payments", "reconciliation"],
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
