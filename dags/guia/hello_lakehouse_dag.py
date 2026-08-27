"""HOLA LAKEHOUSE — el DAG más chico que usa las tres piezas."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

LAKEHOUSE = "hdfs://hdfs-namenode:9000/lakehouse"


def write_greeting(run_date: str) -> None:
    """Crea dos filas en Spark y las deja en HDFS como Parquet."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("hello-lakehouse")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )
    try:
        rows = spark.createDataFrame(
            [("hola", 1), ("lakehouse", 2)], "palabra string, orden int"
        )
        rows.write.mode("overwrite").parquet(f"{LAKEHOUSE}/hello/run_date={run_date}")
    finally:
        spark.stop()


with DAG(
    dag_id="hello_lakehouse",
    description="Airflow dispara, Spark calcula, HDFS guarda",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"owner": "aprendiz", "retries": 0},
    tags=["guia", "hola"],
) as dag:
    PythonOperator(
        task_id="write_greeting",
        python_callable=write_greeting,
        op_kwargs={"run_date": "{{ ds }}"},
    )