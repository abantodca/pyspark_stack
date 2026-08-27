"""MEDALLION v0 — Customer 360 escrito a mano, sin infraestructura compartida."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

SAMPLE_SCHEMA = (
    "customer_id string, full_name string, email string, segment string, "
    "updated_at string, lifetime_value double"
)
SAMPLE_CUSTOMERS = [
    ("C001", "Ana Torres", "ana@example.com", "retail", "2026-01-05T10:00:00Z", 1250.50),
    ("C002", "Luis Pérez", "luis@example.com", "business", "2026-01-05T11:00:00Z", 4890.00),
]


def bronze_ingest(run_date: str) -> None:
    """Aterriza el maestro CRM tal como llega."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("customer_360_v0-bronze")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )
    try:
        source = spark.createDataFrame(SAMPLE_CUSTOMERS, SAMPLE_SCHEMA)
        bronze = source.withColumn("_ingested_at", F.current_timestamp())
        bronze.write.mode("overwrite").parquet(
            f"hdfs://hdfs-namenode:9000/lakehouse/bronze/customer_360/run_date={run_date}"
        )
    finally:
        spark.stop()


def silver_conform(run_date: str) -> None:
    """Tipifica y filtra lo que no sirve."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("customer_360_v0-silver")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )
    try:
        silver = (
            spark.read.parquet(
                f"hdfs://hdfs-namenode:9000/lakehouse/bronze/customer_360/run_date={run_date}"
            )
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("lifetime_value", F.col("lifetime_value").cast("decimal(18,2)"))
            .withColumn("email", F.lower(F.trim("email")))
            .withColumn("segment", F.lower(F.trim("segment")))
            .filter(F.col("customer_id").isNotNull())
            .filter(F.col("lifetime_value") >= 0)
        )
        silver.write.mode("overwrite").parquet(
            f"hdfs://hdfs-namenode:9000/lakehouse/silver/customer_360/run_date={run_date}"
        )
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Agrega valor de vida por segmento."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("customer_360_v0-gold")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )
    try:
        gold = (
            spark.read.parquet(
                f"hdfs://hdfs-namenode:9000/lakehouse/silver/customer_360/run_date={run_date}"
            )
            .groupBy("segment")
            .agg(
                F.countDistinct("customer_id").alias("active_customers"),
                F.round(F.sum("lifetime_value"), 2).alias("total_lifetime_value"),
            )
        )
        gold.write.mode("overwrite").parquet(
            f"hdfs://hdfs-namenode:9000/lakehouse/gold/customer_360/run_date={run_date}"
        )
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_customer_360_v0",
    description="Primera versión: correcta a medias y sin nada compartido",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args={"owner": "aprendiz", "retries": 0},
    tags=["guia", "medallion", "v0"],
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
