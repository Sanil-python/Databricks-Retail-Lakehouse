# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@retailsalesanalysissanil.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying function using SQL 

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function rsa_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, rsa_cata.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying function using pyspark

# COMMAND ----------

df.withColumn("discounted_price", expr("rsa_cata.bronze.discount_func(price)")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating function using Python language

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function rsa_cata.bronze.upper_func(p_brand string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, brand, rsa_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table Creation to top of the Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists rsa_cata.silver.products_silver 
# MAGIC using delta
# MAGIC location 'abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/products'