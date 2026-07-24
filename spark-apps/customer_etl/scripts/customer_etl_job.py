import sys

from pyspark.sql import SparkSession


def main(env, run_date, hdfs_input, hdfs_output):
    print(f"[INFO] env = {env}")
    print(f"[INFO] run_date = {run_date}")
    print(f"[INFO] hdfs_input = {hdfs_input}")
    print(f"[INFO] hdfs_output = {hdfs_output}")

    spark = (
        SparkSession.builder.appName("CustomerLoyaltyETL")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    df_orders = spark.read.option("header", True).csv(
        f"hdfs://hdfs-namenode:9000{hdfs_input}/orders.csv"
    )
    df_products = spark.read.option("multiline", "true").json(
        f"hdfs://hdfs-namenode:9000{hdfs_input}/products.json"
    )
    df_customers = spark.read.option("header", True).csv(
        f"hdfs://hdfs-namenode:9000{hdfs_input}/customers.csv"
    )

    df_orders.createOrReplaceTempView("orders")
    df_products.createOrReplaceTempView("products")
    df_customers.createOrReplaceTempView("customers")

    # Enrich with price
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW enriched_orders AS
        SELECT
            o.order_id,
            o.customer_id,
            o.product_id,
            o.quantity,
            o.order_date,
            p.category,
            p.unit_price,
            o.quantity * p.unit_price AS total_price
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
    """)

    # Aggregate per customer
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW customer_metrics AS
        SELECT
            customer_id,
            COUNT(order_id) AS total_orders,
            SUM(total_price) AS total_spent,
            COUNT(DISTINCT order_date) AS days_active,
            COUNT(DISTINCT category) AS categories_bought
        FROM enriched_orders
        GROUP BY customer_id
    """)

    # Add loyalty label
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW customer_loyalty AS
        SELECT
            m.customer_id,
            c.customer_name,
            c.city,
            c.state,
            c.signup_date,
            m.total_orders,
            m.total_spent,
            m.days_active,
            m.categories_bought,
            CASE
                WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2 THEN 'Premium'
                WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2) THEN 'Engaged'
                ELSE 'Casual'
            END AS loyalty_status
        FROM customer_metrics m
        JOIN customers c ON m.customer_id = c.customer_id
    """)

    df_loyalty = spark.sql("SELECT * FROM customer_loyalty")

    df_loyalty.write.mode("overwrite").option("header", True).csv(
        f"hdfs://hdfs-namenode:9000{hdfs_output}"
    )

    spark.stop()


if __name__ == "__main__":
    # NOTE:
    # This script supports two ways of being called in the course:
    # 1) Early lectures (Section 5):
    #       spark-submit customer_etl_job.py <run_date>
    # 2) Later lectures (env-specific paths, Section 6):
    #       spark-submit customer_etl_job.py <env> <run_date> <hdfs_input> <hdfs_output>
    #
    # The argument handling below makes it compatible with both styles.

    # Early lecture → customer_etl_job.py <run_date>
    if len(sys.argv) == 2:
        run_date = sys.argv[1]
        env = "dev"
        # These defaults must match what's in your original simple script
        hdfs_input = "/customer_etl/input"
        hdfs_output = f"/customer_etl/output/loyalty_snapshot_{run_date}"

    # Later lectures → customer_etl_job.py <env> <run_date> <hdfs_input> <hdfs_output>
    elif len(sys.argv) == 5:
        env = sys.argv[1]
        run_date = sys.argv[2]
        hdfs_input = sys.argv[3]
        hdfs_output = sys.argv[4]

    else:
        print(
            "Usage:\n"
            "  spark-submit customer_etl_job.py <run_date>\n"
            "  (OR)\n"
            "  spark-submit customer_etl_job.py <env> <run_date> <hdfs_input> <hdfs_output>"
        )
        sys.exit(1)

    main(env, run_date, hdfs_input, hdfs_output)
