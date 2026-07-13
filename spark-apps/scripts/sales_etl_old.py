from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SalesETLJob") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df_sales = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/sales_etl/input/sales.csv")
    df_products = spark.read.option("multiline", "true").json("hdfs://hdfs-namenode:9000/sales_etl/input/products.json")

    df_sales.createOrReplaceTempView("sales_raw")
    df_products.createOrReplaceTempView("products_raw")

    ## Clean & Transform via Spark SQL

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

    ## Join + Enrich with Classification Logic

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

    ##  Write Output to HDFS Using `write`

    df_output = spark.sql("""
		SELECT sale_id, product_id, quantity, total_price, sale_value_category
		FROM sales_enriched
	""")

    df_output.write.mode("overwrite").option("header", True).csv("hdfs://hdfs-namenode:9000/sales_etl/output/final_csv")
    df_output.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/sales_etl/output/final_parquet")

    spark.stop()

if __name__ == "__main__":
    main()
