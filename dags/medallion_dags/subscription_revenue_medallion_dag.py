"""MEDALLION E2E — Pipeline Subscription Revenue."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from medallion import MedallionRuntime

PROJECT = "subscription_revenue"
RUNTIME = MedallionRuntime(PROJECT)

# Reporte de ingresos: un uno por ciento de eventos sin conciliar detiene el lote.
MAX_REJECT_RATIO = 0.01
MONTHS_PER_YEAR = 12
KNOWN_EVENT_TYPES = ["created", "upgraded", "downgraded", "renewed", "cancelled"]
# Cómo mueve cada evento del ciclo de vida el ingreso recurrente.
MRR_MOVEMENTS = {
    "created": "new_mrr",
    "upgraded": "expansion_mrr",
    "downgraded": "contraction_mrr",
    "cancelled": "churned_mrr",
}


def bronze_ingest(run_date: str) -> None:
    """Land subscription CDC events, invoices, customer accounts and daily FX."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("bronze")
    try:
        events_uri = os.getenv("SUBSCRIPTION_EVENTS_SOURCE_URI")
        invoices_uri = os.getenv("SUBSCRIPTION_INVOICES_SOURCE_URI")
        accounts_uri = os.getenv("SUBSCRIPTION_ACCOUNTS_SOURCE_URI")
        fx_uri = os.getenv("SUBSCRIPTION_FX_SOURCE_URI")
        # Sin URIs configuradas corre con estas filas: S-1002 se crea y se
        # cancela el mismo día, que es el caso que compacta el CDC.
        events = (
            spark.read.json(events_uri)
            if events_uri
            else spark.createDataFrame(
                [
                    ("EV-1001", "S-1001", "C-1001", "created", f"{run_date}T08:00:00Z", "pro", "active", 499.0, "PEN", 1),
                    ("EV-1002", "S-1002", "C-1002", "created", f"{run_date}T08:05:00Z", "basic", "active", 99.0, "USD", 1),
                    ("EV-1003", "S-1002", "C-1002", "cancelled", f"{run_date}T18:00:00Z", "basic", "cancelled", 99.0, "USD", 2),
                ],
                "event_id string, subscription_id string, customer_id string, "
                "event_type string, effective_at string, plan string, status string, "
                "mrr double, currency string, source_sequence bigint",
            )
        )
        invoices = (
            spark.read.json(invoices_uri)
            if invoices_uri
            else spark.createDataFrame(
                [
                    ("INV-1001", "S-1001", "C-1001", run_date, "paid", 499.0, 90.0, "PEN"),
                    ("INV-1002", "S-1002", "C-1002", run_date, "open", 99.0, 0.0, "USD"),
                ],
                "invoice_id string, subscription_id string, customer_id string, "
                "invoice_date string, invoice_status string, gross_amount double, "
                "tax_amount double, currency string",
            )
        )
        accounts = (
            spark.read.json(accounts_uri)
            if accounts_uri
            else spark.createDataFrame(
                [("C-1001", "enterprise", "PE"), ("C-1002", "smb", "US")],
                "customer_id string, segment string, billing_country string",
            )
        )
        fx = (
            spark.read.option("header", True).csv(fx_uri)
            if fx_uri
            else spark.createDataFrame(
                [(run_date, "PEN", 0.27), (run_date, "USD", 1.00)],
                "rate_date string, currency string, usd_rate double",
            )
        )
        for name, source in {
            "events": events,
            "invoices": invoices,
            "accounts": accounts,
            "fx": fx,
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


def silver_revenue_model(run_date: str) -> None:
    """Compact CDC, normalize currency and reconcile subscriptions to invoices."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("silver")
    try:
        events = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "events")
        ).select(
            "event_id",
            "subscription_id",
            "customer_id",
            F.lower("event_type").alias("event_type"),
            F.to_timestamp("effective_at").alias("effective_at"),
            F.lower("plan").alias("plan"),
            F.lower("status").alias("status"),
            F.col("mrr").cast("decimal(18,2)").alias("mrr"),
            F.upper("currency").alias("currency"),
            F.col("source_sequence").cast("long").alias("source_sequence"),
            "_ingested_at",
        ).cache()
        invoices = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "invoices")
        ).select(
            "invoice_id",
            "subscription_id",
            "customer_id",
            F.to_date("invoice_date").alias("invoice_date"),
            F.lower("invoice_status").alias("invoice_status"),
            F.col("gross_amount").cast("decimal(18,2)").alias("gross_amount"),
            F.col("tax_amount").cast("decimal(18,2)").alias("tax_amount"),
            F.upper("currency").alias("invoice_currency"),
        )
        accounts = spark.read.parquet(
            RUNTIME.path("bronze", run_date, "accounts")
        ).select(
            "customer_id",
            F.lower("segment").alias("segment"),
            F.upper("billing_country").alias("billing_country"),
        )
        fx = spark.read.parquet(RUNTIME.path("bronze", run_date, "fx")).select(
            F.to_date("rate_date").alias("rate_date"),
            F.upper("currency").alias("fx_currency"),
            F.col("usd_rate").cast("decimal(18,6)").alias("usd_rate"),
        )
        invalid_events = events.filter(
            F.col("event_id").isNull()
            | F.col("subscription_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("effective_at").isNull()
            | (F.col("mrr") < 0)
            | ~F.col("event_type").isin(KNOWN_EVENT_TYPES)
        )
        clean_events = events.join(
            invalid_events.select("event_id"), "event_id", "left_anti"
        )
        # La secuencia de origen decide el ganador; los timestamps solo desempatan.
        latest_window = Window.partitionBy("subscription_id").orderBy(
            F.col("source_sequence").desc(),
            F.col("effective_at").desc(),
            F.col("_ingested_at").desc(),
        )
        latest = (
            clean_events.withColumn("_rn", F.row_number().over(latest_window))
            .filter("_rn = 1")
            .drop("_rn", "_ingested_at")
        )
        event_metrics = clean_events.groupBy("subscription_id").agg(
            F.sum((F.col("event_type") == "created").cast("int")).alias(
                "created_events"
            ),
            F.sum((F.col("event_type") == "cancelled").cast("int")).alias(
                "cancelled_events"
            ),
            F.max("source_sequence").alias("latest_source_sequence"),
        )
        invoice_revenue = (
            invoices.join(
                fx,
                (invoices.invoice_date == fx.rate_date)
                & (invoices.invoice_currency == fx.fx_currency),
                "left",
            )
            .withColumn("net_amount", F.col("gross_amount") - F.col("tax_amount"))
            .withColumn(
                "net_revenue_usd", F.round(F.col("net_amount") * F.col("usd_rate"), 2)
            )
            .groupBy("subscription_id", "customer_id")
            .agg(
                F.round(F.sum("net_revenue_usd"), 2).alias("invoiced_revenue_usd"),
                F.sum((F.col("invoice_status") == "paid").cast("int")).alias(
                    "paid_invoices"
                ),
                F.countDistinct("invoice_id").alias("invoices"),
                F.sum(F.col("usd_rate").isNull().cast("int")).alias("missing_fx_rows"),
            )
        )
        model = (
            latest.join(
                fx,
                (F.to_date(latest.effective_at) == fx.rate_date)
                & (latest.currency == fx.fx_currency),
                "left",
            )
            .drop("rate_date", "fx_currency")
            .join(event_metrics, "subscription_id", "left")
            .join(invoice_revenue, ["subscription_id", "customer_id"], "left")
            .join(accounts, "customer_id", "left")
            .withColumn("is_active", F.col("status") == "active")
            .withColumn("mrr_usd", F.round(F.col("mrr") * F.col("usd_rate"), 2))
            .withColumn("arr_usd", F.round(F.col("mrr_usd") * MONTHS_PER_YEAR, 2))
            .withColumn("as_of_date", F.lit(run_date).cast("date"))
        )
        # Ingreso sin cuenta o sin tipo de cambio no se puede reportar en USD.
        missing_reference = model.filter(
            F.col("segment").isNull()
            | F.col("usd_rate").isNull()
            | (F.coalesce(F.col("missing_fx_rows"), F.lit(0)) > 0)
        )
        valid = model.join(
            missing_reference.select("subscription_id"), "subscription_id", "left_anti"
        ).cache()
        received = events.count()
        rejected = invalid_events.count() + missing_reference.count()
        published = valid.count()
        invalid_event_records = invalid_events.withColumn(
            "_reject_reason", F.lit("invalid_subscription_event")
        )
        missing_reference_records = missing_reference.withColumn(
            "_reject_reason", F.lit("missing_account_or_fx_reference")
        )
        RUNTIME.write(invalid_event_records, "quarantine", run_date, "events")
        RUNTIME.write(missing_reference_records, "quarantine", run_date, "references")
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
    """Publish SaaS revenue KPIs and the recurring-revenue movement bridge."""
    from pyspark.sql import functions as F

    spark = RUNTIME.spark("gold")
    try:
        model = spark.read.parquet(RUNTIME.path("silver", run_date))
        active_mrr = F.when(F.col("is_active"), F.col("mrr_usd")).otherwise(0)
        revenue_kpis = model.groupBy(
            "as_of_date", "segment", "plan", "billing_country"
        ).agg(
            F.countDistinct("subscription_id").alias("subscriptions"),
            F.sum(F.col("is_active").cast("int")).alias("active_subscriptions"),
            F.round(F.sum(active_mrr), 2).alias("mrr_usd"),
            F.round(
                F.sum(F.when(F.col("is_active"), F.col("arr_usd")).otherwise(0)), 2
            ).alias("arr_usd"),
            F.round(F.sum("invoiced_revenue_usd"), 2).alias("invoiced_revenue_usd"),
            F.round(
                F.sum("paid_invoices") / F.greatest(F.sum("invoices"), F.lit(1)), 4
            ).alias("invoice_collection_rate"),
        )
        RUNTIME.write(revenue_kpis, "gold", run_date, "revenue_kpis")

        movement_type = F.lit("retained_mrr")
        for event_type, movement in MRR_MOVEMENTS.items():
            movement_type = F.when(
                F.col("event_type") == event_type, movement
            ).otherwise(movement_type)
        mrr_movement_bridge = model.select(
            "as_of_date",
            "subscription_id",
            "customer_id",
            "segment",
            "plan",
            movement_type.alias("movement_type"),
            # Una cancelación resta el ingreso recurrente que se llevó.
            F.when(F.col("event_type") == "cancelled", -F.abs(F.col("mrr_usd")))
            .otherwise(F.col("mrr_usd"))
            .alias("mrr_movement_usd"),
        )
        RUNTIME.write(mrr_movement_bridge, "gold", run_date, "mrr_movement_bridge")
    finally:
        spark.stop()


with DAG(
    dag_id="medallion_subscription_revenue",
    description="Subscription CDC, billing and FX reconciliation to SaaS revenue metrics",
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "revenue-data",
        "retries": 3,
        "retry_delay": timedelta(minutes=4),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["medallion", "subscriptions", "revenue", "cdc"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    silver = PythonOperator(
        task_id="silver_revenue_model",
        python_callable=silver_revenue_model,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    gold = PythonOperator(
        task_id="gold_publish",
        python_callable=gold_publish,
        op_kwargs={"run_date": "{{ ds }}"},
    )
    bronze >> silver >> gold
