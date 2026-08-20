#!/usr/bin/env python
# coding: utf-8
"""Export del notebook: exploración sobre la lógica canónica de customer_etl."""

import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark-apps/customer_etl/scripts")

from transforms import (  # noqa: E402
    build_customer_loyalty,
    read_inputs,
    validate_inputs,
    validate_output,
)


spark = SparkSession.builder.appName("CustomerETLNotebook").getOrCreate()
orders, products, customers = read_inputs(
    spark, "file:///opt/spark-apps/landing/customer_etl"
)
input_metrics = validate_inputs(orders, products, customers)
customer_loyalty = build_customer_loyalty(orders, products, customers)
output_metrics = validate_output(customer_loyalty, input_metrics["active_customers"])

print({**input_metrics, **output_metrics})
customer_loyalty.show(truncate=False)

# Escritura opcional del laboratorio. El pipeline orquestado publica en una ruta por RUN_DATE.
customer_loyalty.write.mode("overwrite").option("header", True).csv(
    "hdfs://hdfs-namenode:9000/customer_etl/output/notebook_preview"
)
