#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("CustomerOrdersJob").getOrCreate()


# In[2]:


customers_path = "/opt/spark-apps/input/customers.csv"
orders_path = "/opt/spark-apps/input/orders.json"
output_path_csv = "/tmp/orders_enriched_csv"
output_path_parquet = "/tmp/orders_enriched_parquet"


# In[3]:


df_customers = spark.read.option("header", True).csv(customers_path)
df_orders = spark.read.json(orders_path)


# In[4]:


df_customers.show()


# In[5]:


df_customers.show(4, False)


# In[6]:


df_orders.show(4)


# In[7]:


df_joined = df_orders.join(df_customers, on="customer_id", how="inner")


# In[8]:


df_joined.show(4)


# In[9]:


df_enriched = df_joined.withColumn(
    "order_type",
    when(df_joined.amount >= 200, "High Value")
    .when(df_joined.amount >= 100, "Medium Value")
    .otherwise("Low Value")
)


# In[10]:


df_enriched.select("order_id", "name", "amount", "order_type").show()


# In[11]:


df_enriched_op = df_enriched.select("order_id", "name", "amount", "order_type")


# In[12]:


df_enriched_op.write.mode("overwrite").option("header", True).csv(output_path_csv)


# In[13]:


df_enriched_op.write.mode("overwrite").parquet(output_path_parquet)


# In[ ]:




