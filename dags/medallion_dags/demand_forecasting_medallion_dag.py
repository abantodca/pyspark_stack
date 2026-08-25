"""MEDALLION E2E — Pipeline Demand Forecasting."""

from __future__ import annotations

import os
from datetime import date, timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "demand_forecasting"
RUNTIME = MedallionRuntime(PROJECT)

MAX_REJECT_RATIO = 0.01
# Días de historia detrás de la media móvil y su dispersión.
TREND_DAYS = 7
HISTORY_DAYS = 14
# Cuánto de un descuento se traduce en demanda adicional.
PROMOTION_ELASTICITY = 1.5


def bronze_ingest(run_date: str) -> None:
    """Land sales history, promotions and current inventory as separate contracts."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        sales_uri = os.getenv("DEMAND_SALES_SOURCE_URI")
        promotions_uri = os.getenv("DEMAND_PROMOTIONS_SOURCE_URI")
        inventory_uri = os.getenv("DEMAND_INVENTORY_SOURCE_URI")
        # Sin URI configurada genera dos semanas de historia: suficiente para
        # que la media y la desviación de 7 días signifiquen algo.
        as_of = date.fromisoformat(run_date)
        fixture_sales = []
        for offset in range(HISTORY_DAYS):
            business_date = (as_of - timedelta(days=HISTORY_DAYS - 1 - offset)).isoformat()
            fixture_sales.append(
                (f"L-{offset:02d}-1", "SKU-1", "LIM-01", business_date, 10 + offset % 4, 59.90)
            )
            fixture_sales.append(
                (f"L-{offset:02d}-2", "SKU-2", "LIM-01", business_date, 5 + offset % 3, 249.00)
            )
        sales = (
            spark.read.option("header", True).csv(sales_uri)
            if sales_uri
            else spark.createDataFrame(
                fixture_sales,
                "sales_line_id string, sku string, warehouse_id string, "
                "business_date string, units bigint, unit_price double",
            )
        )
        promotions = (
            spark.read.json(promotions_uri)
            if promotions_uri
            else spark.createDataFrame(
                [
                    ("PROMO-1", "SKU-1", run_date, run_date, 0.10),
                    ("PROMO-2", "SKU-2", run_date, run_date, 0.00),
                ],
                "promotion_id string, sku string, start_date string, "
                "end_date string, discount_rate double",
            )
        )
        inventory = (
            spark.read.json(inventory_uri)
            if inventory_uri
            else spark.createDataFrame(
                [
                    ("SKU-1", "LIM-01", run_date, 18, 5),
                    ("SKU-2", "LIM-01", run_date, 20, 4),
                ],
                "sku string, warehouse_id string, snapshot_date string, "
                "available_qty bigint, supplier_lead_days bigint",
            )
        )
        for name, source in {
            "sales": sales,
            "promotions": promotions,
            "inventory": inventory,
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


def silver_demand_features(run_date: str) -> None:
    """Conform daily demand and enrich it with promotion and stock context."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        sales = (
            spark.read.parquet(RUNTIME.path("bronze", run_date, "sales"))
            .withColumn("business_date", F.to_date("business_date"))
            .withColumn("units", F.col("units").cast("long"))
            .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
            .cache()
        )
        promotions = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "promotions")
        ).select(
            "promotion_id",
            "sku",
            F.to_date("start_date").alias("start_date"),
            F.to_date("end_date").alias("end_date"),
            F.col("discount_rate").cast("double").alias("discount_rate"),
        )
        inventory = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "inventory")
        ).select(
            "sku",
            "warehouse_id",
            F.to_date("snapshot_date").alias("snapshot_date"),
            F.col("available_qty").cast("long").alias("available_qty"),
            F.col("supplier_lead_days").cast("int").alias("supplier_lead_days"),
        )
        invalid = sales.filter(
            F.col("sales_line_id").isNull()
            | F.col("sku").isNull()
            | F.col("business_date").isNull()
            | (F.col("units") < 0)
            | (F.col("unit_price") < 0)
        )
        clean_sales = sales.join(
            invalid.select("sales_line_id"), "sales_line_id", "left_anti"
        )
        daily = clean_sales.groupBy("sku", "warehouse_id", "business_date").agg(
            F.sum("units").alias("units_sold"),
            F.round(F.sum(F.col("units") * F.col("unit_price")), 2).alias(
                "gross_revenue"
            ),
        )
        enriched = (
            daily.join(
                promotions,
                (daily.sku == promotions.sku)
                & daily.business_date.between(
                    promotions.start_date, promotions.end_date
                ),
                "left",
            )
            .drop(promotions.sku)
            .join(inventory, ["sku", "warehouse_id"], "left")
            .fillna({"discount_rate": 0.0})
            .cache()
        )
        received, rejected, published = (
            sales.count(),
            invalid.count(),
            enriched.count(),
        )
        quarantined = invalid.withColumn(
            "_reject_reason", F.lit("invalid_sales_contract")
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
        RUNTIME.write(enriched, "silver", run_date)
    finally:
        spark.stop()


def gold_publish(run_date: str) -> None:
    """Publish statistical demand features, next-day forecast and reorder proposal."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        by_sku = Window.partitionBy("sku", "warehouse_id")
        chronological = by_sku.orderBy("business_date")
        # Solo días pasados: la demanda de hoy no debe filtrarse en su propio forecast.
        trend = chronological.rowsBetween(-TREND_DAYS, -1)
        features = (
            spark.read.parquet(RUNTIME.path("silver", run_date))
            .withColumn("demand_lag_1d", F.lag("units_sold", 1).over(chronological))
            .withColumn("demand_avg_7d", F.round(F.avg("units_sold").over(trend), 2))
            .withColumn(
                "demand_stddev_7d",
                F.round(F.stddev_pop("units_sold").over(trend), 2),
            )
            .withColumn(
                "_rn",
                F.row_number().over(by_sku.orderBy(F.col("business_date").desc())),
            )
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn(
                "baseline_forecast",
                F.coalesce("demand_avg_7d", "demand_lag_1d", "units_sold"),
            )
            .withColumn(
                "forecast_units_next_day",
                F.ceil(
                    F.col("baseline_forecast")
                    * (
                        F.lit(1.0)
                        + F.col("discount_rate") * F.lit(PROMOTION_ELASTICITY)
                    )
                ),
            )
            .withColumn(
                "safety_stock",
                F.ceil(
                    F.coalesce("demand_stddev_7d", F.lit(0.0))
                    * F.sqrt(F.col("supplier_lead_days"))
                ),
            )
            .withColumn(
                "reorder_qty",
                F.greatest(
                    F.lit(0),
                    F.col("forecast_units_next_day") * F.col("supplier_lead_days")
                    + F.col("safety_stock")
                    - F.col("available_qty"),
                ),
            )
            .withColumn(
                "stockout_risk",
                F.col("available_qty") < F.col("forecast_units_next_day"),
            )
            .withColumn("forecast_date", F.date_add(F.lit(run_date).cast("date"), 1))
        )
        RUNTIME.write(features, "gold", run_date, "sku_forecast")
        replenishment_summary = features.groupBy("warehouse_id").agg(
            F.countDistinct("sku").alias("forecasted_skus"),
            F.sum(F.col("stockout_risk").cast("int")).alias("skus_at_stockout_risk"),
            F.sum("reorder_qty").alias("proposed_reorder_units"),
        )
        RUNTIME.write(replenishment_summary, "gold", run_date, "replenishment_summary")
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_demand_forecasting",
    description="Sales, promotion and inventory demand-sensing with replenishment proposals",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "demand-planning",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "forecasting", "inventory", "feature-engineering"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_demand_features",
        python_callable=silver_demand_features,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
