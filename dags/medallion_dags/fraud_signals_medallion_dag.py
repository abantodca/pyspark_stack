"""MEDALLION E2E — Pipeline Fraud Signals."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "fraud_signals"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.01
ALERT_WINDOW = "15 minutes"
# Umbrales de score, de la banda más alta a la más baja.
RISK_BANDS = (("critical", 0.85), ("high", 0.60), ("medium", 0.30))
SOURCE_ENV_VAR = "FRAUD_SIGNALS_SOURCE_URI"
SAMPLE_SCHEMA = (
    "alert_id string, payment_id string, payment_method string, country string, "
    "detected_at string, risk_score double, transaction_amount double"
)
SAMPLE_ALERTS = [
    ("A-901", "P-1001", "card", "PE", "2026-01-05T15:00:00Z", 0.18, 120.50),
    ("A-902", "P-1002", "card", "US", "2026-01-05T15:01:00Z", 0.92, 3800.00),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza las alertas scoreadas junto a la versión del modelo que las produjo."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(uri)
            if uri
            else spark.createDataFrame(SAMPLE_ALERTS, SAMPLE_SCHEMA)
        )
        hashed = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("fraud_scoring_engine"))
            .withColumn("_model_version", F.lit("risk-v3.2"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *hashed), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Rechaza scores fuera de rango, deduplica alertas y asigna la banda de riesgo."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("detected_at", F.to_timestamp("detected_at"))
            .withColumn("risk_score", F.col("risk_score").cast("double"))
            .withColumn(
                "transaction_amount", F.col("transaction_amount").cast("decimal(18,2)")
            )
            .withColumn("country", F.upper(F.trim("country")))
            .withColumn("payment_method", F.lower(F.trim("payment_method")))
        )
        reason = (
            F.when(
                F.col("alert_id").isNull() | F.col("payment_id").isNull(),
                "missing_business_key",
            )
            .when(F.col("detected_at").isNull(), "invalid_detected_at")
            .when(~F.col("risk_score").between(0.0, 1.0), "risk_score_out_of_range")
            .when(F.col("transaction_amount") < 0, "negative_amount")
        )
        # La banda se arma desde la más baja para que la más alta quede arriba.
        risk_band = F.lit("low")
        for band, threshold in reversed(RISK_BANDS):
            risk_band = F.when(F.col("risk_score") >= threshold, band).otherwise(
                risk_band
            )
        # Cacheado: los tres counts de abajo releerían Parquet y rehashearían.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("alert_id").orderBy(
            F.col("detected_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("risk_band", risk_band)
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
    """Publica exposición por ventana, banda de riesgo, país y método de pago."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = spark.read.parquet(RUNTIME.path("silver", run_date)).groupBy(
            F.window("detected_at", ALERT_WINDOW).alias("time_window"),
            "risk_band",
            "country",
            "payment_method",
        ).agg(
            F.countDistinct("alert_id").alias("alerts"),
            F.round(F.sum("transaction_amount"), 2).alias("exposed_amount"),
            F.round(F.avg("risk_score"), 4).alias("avg_risk_score"),
            F.max("risk_score").alias("max_risk_score"),
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_fraud_signals",
    description="Fraud model signals to investigation-ready risk aggregates",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    # Corre cada 15 minutos: un run colgado no debe bloquear la hora siguiente.
    dagrun_timeout=timedelta(minutes=45),
    default_args={
        "owner": "risk-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["medallion", "fraud", "risk"],
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
