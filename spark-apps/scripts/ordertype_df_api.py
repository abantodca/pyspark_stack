
from pyspark.sql import SparkSession
from pyspark.sql.functions import when


def main():

    #initaize spark session
    spark = SparkSession.builder.appName("CustomerOrdersJob").getOrCreate()
    
    
    # set the path
    customers_path = "/opt/spark-apps/input/customers.csv"
    orders_path = "/opt/spark-apps/input/orders.json"
    output_path_csv = "/tmp/orders_enriched_csv"
    output_path_parquet = "/tmp/orders_enriched_parquet"
    
    
    #read the source files
    df_customers = spark.read.option("header", True).csv(customers_path)
    df_orders = spark.read.json(orders_path)
      
    df_orders.show(4)
    
    #apply transfromations
    
    df_joined = df_orders.join(df_customers, on="customer_id", how="inner")
    df_joined.show(4)
    
    
    df_enriched = df_joined.withColumn(
        "order_type",
        when(df_joined.amount >= 200, "High Value")
        .when(df_joined.amount >= 100, "Medium Value")
        .otherwise("Low Value")
    )
    
    
    df_enriched.select("order_id", "name", "amount", "order_type").show()
    df_enriched_op = df_enriched.select("order_id", "name", "amount", "order_type")
    
    # write to output
    df_enriched_op.write.mode("overwrite").option("header", True).csv(output_path_csv)
    
    df_enriched_op.write.mode("overwrite").parquet(output_path_parquet)


    spark.stop()

if __name__ == "__main__":
    main()



