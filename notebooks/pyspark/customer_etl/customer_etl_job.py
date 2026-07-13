#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CustomerETL").getOrCreate()


# In[2]:


df_orders = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/customer_etl/input/orders.csv")
df_products = spark.read.json("hdfs://hdfs-namenode:9000/customer_etl/input/products.json")
df_customers = spark.read.option("header", True).csv("hdfs://hdfs-namenode:9000/customer_etl/input/customers.csv")


# In[3]:


df_orders.show()


# In[4]:


df_orders.createOrReplaceTempView("orders")
df_products.createOrReplaceTempView("products")
df_customers.createOrReplaceTempView("customers")


# In[5]:


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
            WHEN m.total_orders >= 3 AND m.days_active >= 2 AND m.categories_bought >= 2 THEN 'Loyal'
            WHEN m.total_orders >= 2 AND (m.days_active >= 2 OR m.categories_bought >= 2) THEN 'Engaged'
            ELSE 'Casual'
        END AS loyalty_status
    FROM customer_metrics m
    JOIN customers c ON m.customer_id = c.customer_id
""")


# In[6]:


df_loyalty = spark.sql("SELECT * FROM customer_loyalty")


# In[7]:


df_loyalty.show()


# In[8]:


df_loyalty.write.mode("overwrite").option("header", True).csv("hdfs://hdfs-namenode:9000/customer_etl/output/loyalty_snapshot")


# In[ ]:




