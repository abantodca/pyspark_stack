"""MEDALLION E2E — Pipeline Order Fulfillment OTIF."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "order_fulfillment_otif"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.01
SECONDS_PER_HOUR = 3600


def bronze_ingest(run_date: str) -> None:
    """Land OMS orders, WMS fulfillment and carrier delivery events."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        orders_uri = os.getenv("OTIF_ORDERS_SOURCE_URI")
        fulfillment_uri = os.getenv("OTIF_FULFILLMENT_SOURCE_URI")
        delivery_uri = os.getenv("OTIF_DELIVERY_SOURCE_URI")
        # Sin URIs configuradas corre con estas filas: el segundo pedido llega
        # incompleto y fuera de fecha, que es el caso que mide el OTIF.
        orders = (
            spark.read.json(orders_uri)
            if orders_uri
            else spark.createDataFrame(
                [
                    ("O-1001", "C-1001", "LIM-01", f"{run_date}T08:00:00Z", run_date, 10, 500.0),
                    ("O-1002", "C-1002", "LIM-01", f"{run_date}T08:10:00Z", run_date, 8, 800.0),
                ],
                "order_id string, customer_id string, warehouse_id string, "
                "ordered_at string, promised_date string, ordered_units bigint, "
                "order_value double",
            )
        )
        fulfillment = (
            spark.read.json(fulfillment_uri)
            if fulfillment_uri
            else spark.createDataFrame(
                [
                    ("F-1001", "O-1001", f"{run_date}T09:00:00Z", f"{run_date}T11:00:00Z", 10),
                    ("F-1002", "O-1002", f"{run_date}T10:00:00Z", f"{run_date}T14:00:00Z", 6),
                ],
                "fulfillment_id string, order_id string, picked_at string, "
                "shipped_at string, shipped_units bigint",
            )
        )
        delivery = (
            spark.read.option("header", True).csv(delivery_uri)
            if delivery_uri
            else spark.createDataFrame(
                [
                    ("S-1001", "O-1001", "carrier-a", f"{run_date}T18:00:00Z", 10, "delivered"),
                    ("S-1002", "O-1002", "carrier-b", f"{run_date}T23:00:00Z", 6, "delivered"),
                ],
                "shipment_id string, order_id string, carrier string, "
                "delivered_at string, delivered_units bigint, delivery_status string",
            )
        )
        for name, source in {
            "orders": orders,
            "fulfillment": fulfillment,
            "delivery": delivery,
        }.items():
            values = [
                F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                for c in sorted(source.columns)
            ]
            bronze = (
                source.withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_dataset", F.lit(name))
                .withColumn("_contract_version", F.lit("1.0.0"))
                .withColumn("_record_hash", F.sha2(F.concat_ws("||", *values), 256))
            )
            RUNTIME.write(bronze, "bronze", run_date, name)
    finally:
        spark.stop()


def silver_order_lifecycle(run_date: str) -> None:
    """Reconcile units and create one conformed lifecycle row per order."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:

        def latest_per_order(frame, recency: str):
            """Una sola fila por pedido: la más reciente del sistema de origen."""
            window = Window.partitionBy("order_id").orderBy(
                F.col(recency).desc(), F.col("_ingested_at").desc()
            )
            return (
                frame.withColumn("_rn", F.row_number().over(window))
                .filter("_rn = 1")
                .drop("_rn", "_ingested_at")
            )

        def elapsed_hours(start: str, end: str):
            return F.round(
                (F.unix_timestamp(end) - F.unix_timestamp(start)) / SECONDS_PER_HOUR, 2
            )

        orders = spark.read.parquet(RUNTIME.path("bronze", run_date, "orders")).select(
            "order_id",
            "customer_id",
            "warehouse_id",
            F.to_timestamp("ordered_at").alias("ordered_at"),
            F.to_date("promised_date").alias("promised_date"),
            F.col("ordered_units").cast("long").alias("ordered_units"),
            F.col("order_value").cast("decimal(18,2)").alias("order_value"),
        ).cache()
        fulfillment = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "fulfillment")
        ).select(
            "fulfillment_id",
            "order_id",
            F.to_timestamp("picked_at").alias("picked_at"),
            F.to_timestamp("shipped_at").alias("shipped_at"),
            F.col("shipped_units").cast("long").alias("shipped_units"),
            "_ingested_at",
        )
        delivery = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "delivery")
        ).select(
            "shipment_id",
            "order_id",
            F.lower("carrier").alias("carrier"),
            F.to_timestamp("delivered_at").alias("delivered_at"),
            F.col("delivered_units").cast("long").alias("delivered_units"),
            F.lower("delivery_status").alias("delivery_status"),
            "_ingested_at",
        )
        lifecycle = (
            orders.join(latest_per_order(fulfillment, "shipped_at"), "order_id", "left")
            .join(latest_per_order(delivery, "delivered_at"), "order_id", "left")
            .withColumn(
                "fill_rate",
                F.round(F.col("delivered_units") / F.col("ordered_units"), 4),
            )
            .withColumn("on_time", F.to_date("delivered_at") <= F.col("promised_date"))
            .withColumn("in_full", F.col("delivered_units") >= F.col("ordered_units"))
            .withColumn("otif", F.col("on_time") & F.col("in_full"))
            .withColumn(
                "order_to_ship_hours", elapsed_hours("ordered_at", "shipped_at")
            )
            .withColumn("transit_hours", elapsed_hours("shipped_at", "delivered_at"))
            .withColumn(
                "failure_reason",
                F.when(F.col("delivered_at").isNull(), "not_delivered")
                .when(~F.col("in_full"), "short_shipment")
                .when(~F.col("on_time"), "late_delivery")
                .otherwise("none"),
            )
        )
        # Las unidades solo pueden bajar: pedidas >= enviadas >= entregadas.
        invalid = lifecycle.filter(
            F.col("order_id").isNull()
            | F.col("ordered_at").isNull()
            | F.col("promised_date").isNull()
            | (F.col("ordered_units") <= 0)
            | (F.col("shipped_units") < 0)
            | (F.col("delivered_units") < 0)
            | (F.col("shipped_units") > F.col("ordered_units"))
            | (F.col("delivered_units") > F.col("shipped_units"))
        )
        valid = lifecycle.join(
            invalid.select("order_id"), "order_id", "left_anti"
        ).cache()
        received, rejected, published = orders.count(), invalid.count(), valid.count()
        quarantined = invalid.withColumn(
            "_reject_reason", F.lit("invalid_order_or_unit_reconciliation")
        )
        RUNTIME.write(quarantined, "quarantine", run_date)
        RUNTIME.enforce_quality(
            spark,
            run_date,
            received=received,
            rejected=rejected,
            published=published,
            max_rejected_ratio=MAX_REJECT_RATIO,
        )
        RUNTIME.write(valid, "silver", run_date)
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Publish carrier/warehouse scorecards and actionable order exceptions."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        lifecycle = spark.read.parquet(RUNTIME.path("silver", run_date))
        otif_scorecard = lifecycle.groupBy(
            F.to_date("ordered_at").alias("order_date"), "warehouse_id", "carrier"
        ).agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.avg(F.col("otif").cast("double")), 4).alias("otif_rate"),
            F.round(F.avg("fill_rate"), 4).alias("avg_fill_rate"),
            F.round(F.avg("order_to_ship_hours"), 2).alias("avg_order_to_ship_hours"),
            F.round(F.avg("transit_hours"), 2).alias("avg_transit_hours"),
            F.round(
                F.sum(F.when(~F.col("otif"), F.col("order_value")).otherwise(0)), 2
            ).alias("revenue_at_risk"),
        )
        order_exceptions = lifecycle.filter(
            ~F.col("otif") | F.col("delivered_at").isNull()
        ).select(
            "order_id",
            "customer_id",
            "warehouse_id",
            "carrier",
            "promised_date",
            "failure_reason",
            "order_value",
        )
        RUNTIME.write(otif_scorecard, "gold", run_date, "otif_scorecard")
        RUNTIME.write(order_exceptions, "gold", run_date, "order_exceptions")
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_order_fulfillment_otif",
    description="OMS, WMS and carrier lifecycle reconciliation with OTIF scorecards",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "logistics-analytics",
        "retries": 2,
        "retry_delay": timedelta(minutes=4),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "logistics", "otif", "reconciliation"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_order_lifecycle",
        python_callable=silver_order_lifecycle,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
