"""MEDALLION E2E — Pipeline Customer 360."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "customer_360"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.05
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


SOURCE_ENV_VAR = "CUSTOMER_360_SOURCE_URI"
SAMPLE_SCHEMA = (
    "customer_id string, full_name string, email string, segment string, "
    "updated_at string, lifetime_value double"
)
SAMPLE_CUSTOMERS = [
    (
        "C001",
        "Ana Torres",
        "ana@example.com",
        "retail",
        "2026-01-05T10:00:00Z",
        1250.50,
    ),
    (
        "C002",
        "Luis Pérez",
        "luis@example.com",
        "business",
        "2026-01-05T11:00:00Z",
        4890.00,
    ),
]


def bronze_ingest(run_date: str) -> None:
    """Captura el maestro CRM sin alterar y agrega metadata de linaje."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        source_uri = os.getenv(SOURCE_ENV_VAR)
        source = (
            spark.read.json(source_uri)
            if source_uri
            else spark.createDataFrame(SAMPLE_CUSTOMERS, SAMPLE_SCHEMA)
        )
        columns = [
            F.coalesce(F.col(c).cast("string"), F.lit("∅"))
            for c in sorted(source.columns)
        ]
        bronze = (
            source.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit("crm_customer_master"))
            .withColumn("_contract_version", F.lit("1.0.0"))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *columns), 256))
        )
        RUNTIME.write(bronze, "bronze", run_date)
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Valida PII mínima, tipifica, deduplica y separa registros rechazados."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        frame = (
            spark.read.parquet(RUNTIME.path("bronze", run_date))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("lifetime_value", F.col("lifetime_value").cast("decimal(18,2)"))
            .withColumn("email", F.lower(F.trim("email")))
            .withColumn("segment", F.lower(F.trim("segment")))
        )
        reason = (
            F.when(
                F.col("customer_id").isNull() | (F.trim("customer_id") == ""),
                "missing_customer_id",
            )
            .when(~F.col("email").rlike(EMAIL_PATTERN), "invalid_email")
            .when(F.col("updated_at").isNull(), "invalid_updated_at")
            .when(
                F.col("lifetime_value").isNull() | (F.col("lifetime_value") < 0),
                "invalid_lifetime_value",
            )
        )
        # Cacheado: los tres counts de abajo recalcularían el hash y la ventana.
        checked = frame.withColumn("_reject_reason", reason).cache()
        rejected = checked.filter(F.col("_reject_reason").isNotNull())
        accepted = checked.filter(F.col("_reject_reason").isNull()).drop(
            "_reject_reason"
        )
        window = Window.partitionBy("customer_id").orderBy(
            F.col("updated_at").desc(), F.col("_ingested_at").desc()
        )
        silver = (
            accepted.withColumn("_rn", F.row_number().over(window))
            .filter("_rn = 1")
            .drop("_rn")
            .cache()
        )
        received, rejected_count, published = (
            checked.count(),
            rejected.count(),
            silver.count(),
        )
        RUNTIME.write(rejected, "quarantine", run_date)
        RUNTIME.enforce_quality(
            spark,
            run_date,
            received=received,
            rejected=rejected_count,
            published=published,
            max_rejected_ratio=MAX_REJECT_RATIO,
        )
        RUNTIME.write(silver, "silver", run_date)
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Publica la tabla de consumo de valor y distribución por segmento."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        gold = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .groupBy("segment")
            .agg(
                F.countDistinct("customer_id").alias("active_customers"),
                F.round(F.sum("lifetime_value"), 2).alias("total_lifetime_value"),
                F.round(F.avg("lifetime_value"), 2).alias("avg_lifetime_value"),
            )
            .withColumn("as_of_date", F.lit(run_date).cast("date"))
        )
        RUNTIME.write(gold, "gold", run_date)
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_customer_360",
    description="CRM customer master to governed Customer 360 serving model",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    # Un driver Spark colgado no debe retener el único run permitido.
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "customer-data",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "customer", "pii"],
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
