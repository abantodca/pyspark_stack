from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from transforms import (
    CUSTOMERS_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    DataQualityError,
    build_customer_loyalty,
    validate_inputs,
    validate_output,
)


def valid_inputs(spark):
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "P1", 1, date(2026, 8, 17)),
            ("O2", "C1", "P2", 2, date(2026, 8, 18)),
            ("O3", "C1", "P1", 1, date(2026, 8, 18)),
            ("O4", "C2", "P1", 1, date(2026, 8, 18)),
        ],
        ORDERS_SCHEMA,
    )
    products = spark.createDataFrame(
        [
            ("P1", "Books", Decimal("10.00")),
            ("P2", "Tech", Decimal("20.00")),
        ],
        PRODUCTS_SCHEMA,
    )
    customers = spark.createDataFrame(
        [
            ("C1", "One", "Bogota", "DC", date(2025, 1, 1)),
            ("C2", "Two", "Medellin", "AN", date(2025, 2, 1)),
        ],
        CUSTOMERS_SCHEMA,
    )
    return orders, products, customers


def test_customer_loyalty_business_rules(spark) -> None:
    orders, products, customers = valid_inputs(spark)
    metrics = validate_inputs(orders, products, customers)
    result = build_customer_loyalty(orders, products, customers)
    output_metrics = validate_output(result, metrics["active_customers"])

    rows = {row.customer_id: row for row in result.collect()}
    assert rows["C1"].loyalty_status == "Premium"
    assert rows["C1"].total_orders == 3
    assert rows["C1"].total_spent == Decimal("60.00")
    assert rows["C2"].loyalty_status == "Casual"
    assert metrics == {
        "orders": 4,
        "products": 2,
        "customers": 2,
        "active_customers": 2,
    }
    assert output_metrics == {"output_rows": 2}


def test_duplicate_order_is_rejected(spark) -> None:
    orders, products, customers = valid_inputs(spark)
    duplicate = orders.unionByName(orders.filter("order_id = 'O1'"))
    with pytest.raises(DataQualityError, match="order_id duplicado"):
        validate_inputs(duplicate, products, customers)


def test_unknown_dimension_keys_are_rejected(spark) -> None:
    orders, products, customers = valid_inputs(spark)
    unknown = spark.createDataFrame(
        [("O5", "C404", "P404", 1, date(2026, 8, 18))], ORDERS_SCHEMA
    )
    with pytest.raises(DataQualityError, match="sin correspondencia"):
        validate_inputs(orders.unionByName(unknown), products, customers)


def test_blank_business_key_is_rejected(spark) -> None:
    orders, products, customers = valid_inputs(spark)
    blank = spark.createDataFrame(
        [(" ", "C1", "P1", 1, date(2026, 8, 18))], ORDERS_SCHEMA
    )
    with pytest.raises(DataQualityError, match="strings vacíos"):
        validate_inputs(orders.unionByName(blank), products, customers)
