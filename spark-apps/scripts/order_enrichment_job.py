from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def main():
    spark = SparkSession.builder \
        .appName("OrderEnrichmentJob") \
        .getOrCreate()

    # --- 🔁 INPUT PATHS: Choose based on environment ---

    # Dev (Jupyter or PySpark shell)
    # customers_path = "/opt/spark-apps/input/customers.csv"
    # orders_path = "/opt/spark-apps/input/orders.json"

    # Prod-style (HDFS via spark-submit or Airflow)
    customers_path = "hdfs://hdfs-namenode:9000/input/customers.csv"
    orders_path = "hdfs://hdfs-namenode:9000/input/orders.json"

    # --- ✅ READ INPUT FILES ---
    df_customers = spark.read.option("header", True).csv(customers_path)
    df_orders = spark.read.json(orders_path)

    # --- 🔄 JOIN & TRANSFORM ---
    df_joined = df_orders.join(df_customers, on="customer_id", how="inner")

    df_enriched = df_joined.withColumn(
        "order_type",
        when(df_joined.amount >= 200, "High Value")
        .when(df_joined.amount >= 100, "Medium Value")
        .otherwise("Low Value")
    )

    # --- 📤 WRITE OUTPUT TO HDFS ---
    df_enriched.write.mode("overwrite").option("header", True)\
        .csv("hdfs://hdfs-namenode:9000/user/jovyan/output/orders_enriched_csv")

    df_enriched.write.mode("overwrite")\
        .parquet("hdfs://hdfs-namenode:9000/user/jovyan/output/orders_enriched_parquet")

    spark.stop()

if __name__ == "__main__":
    main()
