"""customer_etl para EMR Serverless — S3 in/out, sin HDFS, sin master hardcodeado.

Se sube a s3://<artifacts>/emr/customer_etl.py (deploy, §11.3) y lo ejecuta
EmrServerlessStartJobOperator (dags/customer_etl_emr_dag.py, §9/§10.2).

Args: 1) datalake_bucket (sin s3://)   2) run_date (YYYY-MM-DD, para particionar).
"""

import sys

from pyspark.sql import SparkSession


def main(datalake: str, run_date: str) -> None:
    base = f"s3a://{datalake}"
    raw = f"{base}/raw/customer_etl"
    out = f"{base}/curated/customer_loyalty/dt={run_date}"

    # Sin .master(): EMR Serverless inyecta master/recursos. La config de Spark viaja
    # por-job en sparkSubmitParameters (no hay spark-defaults.conf local en prod).
    spark = SparkSession.builder.appName("CustomerLoyaltyETL").getOrCreate()

    spark.read.option("header", True).csv(f"{raw}/orders.csv").createOrReplaceTempView(
        "orders"
    )
    spark.read.option("multiline", "true").json(
        f"{raw}/products.json"
    ).createOrReplaceTempView("products")
    spark.read.option("header", True).csv(
        f"{raw}/customers.csv"
    ).createOrReplaceTempView("customers")

    df = spark.sql("""
        WITH enriched AS (
            SELECT o.order_id, o.customer_id, o.product_id, o.quantity, o.order_date,
                   p.category, p.unit_price, o.quantity * p.unit_price AS total_price
            FROM orders o JOIN products p ON o.product_id = p.product_id
        ),
        metrics AS (
            SELECT customer_id,
                   COUNT(order_id) AS total_orders,
                   SUM(total_price) AS total_spent,
                   COUNT(DISTINCT order_date) AS days_active,
                   COUNT(DISTINCT category) AS categories_bought
            FROM enriched GROUP BY customer_id
        )
        SELECT m.customer_id, c.customer_name, c.city, c.state, c.signup_date,
               m.total_orders, m.total_spent, m.days_active, m.categories_bought,
               CASE
                   WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2
                       THEN 'Premium'
                   WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2)
                       THEN 'Engaged'
                   ELSE 'Casual'
               END AS loyalty_status
        FROM metrics m JOIN customers c ON m.customer_id = c.customer_id
    """)

    # Parquet particionado por fecha: barato de escanear por Athena (§16, partition projection).
    df.write.mode("overwrite").parquet(out)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: customer_etl.py <datalake_bucket> [run_date]")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "latest")
