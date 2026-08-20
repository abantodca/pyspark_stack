"""Contrato, calidad y transformaciones puras del pipeline customer_etl."""

from __future__ import annotations

import json
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("order_date", DateType(), False),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("signup_date", DateType(), False),
    ]
)


class DataQualityError(ValueError):
    """El lote no cumple el contrato mínimo para publicarse."""


def read_inputs(spark: SparkSession, base_uri: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Lee las tres fuentes con esquemas explícitos y parsing estricto."""
    orders = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(ORDERS_SCHEMA)
        .csv(f"{base_uri}/orders.csv")
    )
    products = (
        spark.read.option("multiline", "true")
        .option("mode", "FAILFAST")
        .schema(PRODUCTS_SCHEMA)
        .json(f"{base_uri}/products.json")
    )
    customers = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(CUSTOMERS_SCHEMA)
        .csv(f"{base_uri}/customers.csv")
    )
    return orders, products, customers


def _has_rows(dataframe: DataFrame) -> bool:
    return bool(dataframe.limit(1).count())


def _duplicate_keys(dataframe: DataFrame, key: str) -> bool:
    return _has_rows(dataframe.groupBy(key).count().filter(F.col("count") > 1))


def validate_inputs(
    orders: DataFrame, products: DataFrame, customers: DataFrame
) -> dict[str, int]:
    """Valida claves, dominios y referencias antes de transformar o publicar."""
    metrics = {
        "orders": orders.count(),
        "products": products.count(),
        "customers": customers.count(),
        "active_customers": orders.select("customer_id").distinct().count(),
    }
    failures: list[str] = []

    for name, dataframe in (
        ("orders", orders),
        ("products", products),
        ("customers", customers),
    ):
        if metrics[name] == 0:
            failures.append(f"{name}: dataset vacío")
        null_predicate = reduce(
            lambda left, right: left | right,
            (F.col(column).isNull() for column in dataframe.columns),
        )
        if _has_rows(dataframe.filter(null_predicate)):
            failures.append(f"{name}: contiene valores nulos")
        string_columns = [
            field.name
            for field in dataframe.schema.fields
            if isinstance(field.dataType, StringType)
        ]
        blank_predicate = reduce(
            lambda left, right: left | right,
            (F.trim(F.col(column)) == "" for column in string_columns),
        )
        if _has_rows(dataframe.filter(blank_predicate)):
            failures.append(f"{name}: contiene strings vacíos")

    if _duplicate_keys(orders, "order_id"):
        failures.append("orders: order_id duplicado")
    if _duplicate_keys(products, "product_id"):
        failures.append("products: product_id duplicado")
    if _duplicate_keys(customers, "customer_id"):
        failures.append("customers: customer_id duplicado")
    if _has_rows(orders.filter(F.col("quantity") <= 0)):
        failures.append("orders: quantity debe ser mayor que cero")
    if _has_rows(products.filter(F.col("unit_price") < 0)):
        failures.append("products: unit_price no puede ser negativo")
    if _has_rows(orders.join(products, "product_id", "left_anti")):
        failures.append("orders: product_id sin correspondencia en products")
    if _has_rows(orders.join(customers, "customer_id", "left_anti")):
        failures.append("orders: customer_id sin correspondencia en customers")

    if failures:
        raise DataQualityError(json.dumps({"quality_failures": failures}, ensure_ascii=False))
    return metrics


def build_customer_loyalty(
    orders: DataFrame, products: DataFrame, customers: DataFrame
) -> DataFrame:
    """Transformación determinista sin I/O: DataFrames de entrada a snapshot de lealtad."""
    enriched = orders.join(products, "product_id", "inner").withColumn(
        "total_price", F.col("quantity") * F.col("unit_price")
    )
    metrics = enriched.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_price").alias("total_spent"),
        F.countDistinct("order_date").alias("days_active"),
        F.countDistinct("category").alias("categories_bought"),
    )
    return (
        metrics.join(customers, "customer_id", "inner")
        .withColumn(
            "loyalty_status",
            F.when(
                (F.col("total_orders") >= 3)
                & (F.col("days_active") >= 2)
                & (F.col("categories_bought") >= 2),
                F.lit("Premium"),
            )
            .when(
                (F.col("total_orders") >= 2)
                & ((F.col("days_active") >= 2) | (F.col("categories_bought") >= 2)),
                F.lit("Engaged"),
            )
            .otherwise(F.lit("Casual")),
        )
        .select(
            "customer_id",
            "customer_name",
            "city",
            "state",
            "signup_date",
            "total_orders",
            "total_spent",
            "days_active",
            "categories_bought",
            "loyalty_status",
        )
    )


def validate_output(
    customer_loyalty: DataFrame, expected_active_customers: int
) -> dict[str, int]:
    """Impide publicar un snapshot vacío, duplicado o incompleto."""
    rows = customer_loyalty.count()
    failures: list[str] = []
    if rows == 0:
        failures.append("customer_loyalty: salida vacía")
    if _duplicate_keys(customer_loyalty, "customer_id"):
        failures.append("customer_loyalty: customer_id duplicado")
    if rows != expected_active_customers:
        failures.append(
            "customer_loyalty: cantidad de filas distinta a clientes activos de entrada"
        )
    if _has_rows(customer_loyalty.filter(F.col("total_spent") < 0)):
        failures.append("customer_loyalty: total_spent negativo")
    if _has_rows(
        customer_loyalty.filter(
            ~F.col("loyalty_status").isin("Premium", "Engaged", "Casual")
        )
    ):
        failures.append("customer_loyalty: loyalty_status fuera de dominio")
    if failures:
        raise DataQualityError(json.dumps({"quality_failures": failures}, ensure_ascii=False))
    return {"output_rows": rows}
