import json
import sys
from datetime import date

from pyspark.sql import SparkSession

from transforms import (
    build_customer_loyalty,
    read_inputs,
    validate_inputs,
    validate_output,
)


def _validate_arguments(env: str, run_date: str, hdfs_input: str, hdfs_output: str) -> None:
    if env not in {"dev", "prod"}:
        raise ValueError("env debe ser 'dev' o 'prod'")
    date.fromisoformat(run_date)
    for name, value in (("hdfs_input", hdfs_input), ("hdfs_output", hdfs_output)):
        if not value.startswith("/") or ".." in value.split("/"):
            raise ValueError(f"{name} debe ser una ruta HDFS absoluta sin '..'")


def main(env: str, run_date: str, hdfs_input: str, hdfs_output: str) -> None:
    _validate_arguments(env, run_date, hdfs_input, hdfs_output)
    print(f"[INFO] env = {env}")
    print(f"[INFO] run_date = {run_date}")
    print(f"[INFO] hdfs_input = {hdfs_input}")
    print(f"[INFO] hdfs_output = {hdfs_output}")

    spark = (
        SparkSession.builder.appName("CustomerLoyaltyETL")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    base_uri = f"hdfs://hdfs-namenode:9000{hdfs_input}"
    try:
        orders, products, customers = read_inputs(spark, base_uri)
        quality_metrics = validate_inputs(orders, products, customers)
        customer_loyalty = build_customer_loyalty(orders, products, customers)
        quality_metrics.update(
            validate_output(customer_loyalty, quality_metrics["active_customers"])
        )
        print(
            "[QUALITY] "
            + json.dumps(
                {"run_date": run_date, "env": env, **quality_metrics}, sort_keys=True
            )
        )
        (
            customer_loyalty.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(f"hdfs://hdfs-namenode:9000{hdfs_output}")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) == 5:
        env = sys.argv[1]
        run_date = sys.argv[2]
        hdfs_input = sys.argv[3]
        hdfs_output = sys.argv[4]

    else:
        print(
            "Usage: spark-submit customer_etl_job.py "
            "<env> <run_date> <hdfs_input> <hdfs_output>"
        )
        sys.exit(1)

    main(env, run_date, hdfs_input, hdfs_output)
