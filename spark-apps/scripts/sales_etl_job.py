import sys
from pyspark.sql import SparkSession

def main(run_date):
    spark = SparkSession.builder \
        .appName("SalesETLJob") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Input file paths
    df_sales = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/sales_etl/input/sales.csv")
    df_products = spark.read.option("multiline", "true").json("hdfs://hdfs-namenode:9000/sales_etl/input/products.json")

    # Register temp views
    df_sales.createOrReplaceTempView("sales_raw")
    df_products.createOrReplaceTempView("products_raw")

    # Clean sales data
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sales_clean AS
        SELECT
            sale_id,
            product_id,
            CAST(quantity AS INT) AS quantity,
            date
        FROM sales_raw
        WHERE quantity IS NOT NULL
    """)

    # Join + classify
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW sales_enriched AS
        SELECT
            s.sale_id,
            s.product_id,
            s.quantity,
            p.unit_price,
            s.quantity * p.unit_price AS total_price,
            CASE
                WHEN s.quantity * p.unit_price >= 1000 THEN 'High'
                WHEN s.quantity * p.unit_price >= 300 THEN 'Medium'
                ELSE 'Low'
            END AS sale_value_category
        FROM sales_clean s
        JOIN products_raw p
        ON s.product_id = p.product_id
    """)

    # Final output DataFrame
    df_output = spark.sql("""
        SELECT sale_id, product_id, quantity, total_price, sale_value_category
        FROM sales_enriched
    """)

    # Output path with dynamic date
    output_csv_path = f"hdfs://hdfs-namenode:9000/sales_etl/output/csv/{run_date}"
    output_parquet_path = f"hdfs://hdfs-namenode:9000/sales_etl/output/parquet/{run_date}"

    # Write output
    df_output.write.mode("overwrite").option("header", True).csv(output_csv_path)
    df_output.write.mode("overwrite").parquet(output_parquet_path)

    print(f"Job completed for {run_date}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide run_date argument in format YYYY-MM-DD")
        sys.exit(1)

    run_date = sys.argv[1]
    main(run_date)
