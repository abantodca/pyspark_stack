from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    print("🚀 Starting Sales ETL Job")

    spark = SparkSession.builder.appName("SalesETLJob").getOrCreate()

    # Mode: dev or prod
    mode = "dev"

    if mode == "dev":
        sales_path = "/opt/spark-apps/project1/input/sales.csv"
        products_path = "/opt/spark-apps/project1/input/products.json"
        output_csv = "/opt/spark-apps/project1/output/daily_sales_csv"
        output_parquet = "/opt/spark-apps/project1/output/daily_sales_parquet"
    else:
        sales_path = "hdfs://hdfs-namenode:9000/input/sales.csv"
        products_path = "hdfs://hdfs-namenode:9000/input/products.json"
        output_csv = "hdfs://hdfs-namenode:9000/output/daily_sales_csv"
        output_parquet = "hdfs://hdfs-namenode:9000/output/daily_sales_parquet"

    print(f"📥 Reading sales from {sales_path}")
    df_sales = spark.read.option("header", True).csv(sales_path)

    print(f"📥 Reading products from {products_path}")
    df_products = spark.read.option("multiline", "true").json(products_path)

    print("🧹 Cleaning data...")
    df_sales_clean = df_sales.dropna(subset=["quantity"]) \
                             .withColumn("quantity", col("quantity").cast("int"))

    print("🔗 Joining datasets...")
    df_joined = df_sales_clean.join(df_products, on="product_id", how="inner")

    print("➕ Enriching data...")
    df_final = df_joined.withColumn(
        "total_price", col("quantity") * col("unit_price")
    ).withColumn(
        "sale_value_category",
        when(col("total_price") >= 1000, "High")
        .when(col("total_price") >= 300, "Medium")
        .otherwise("Low")
    )

    print(f"📤 Writing CSV to {output_csv}")
    df_final.write.mode("overwrite").option("header", True).csv(output_csv)

    print(f"📤 Writing Parquet to {output_parquet}")
    df_final.write.mode("overwrite").parquet(output_parquet)

    print("✅ Job completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()
