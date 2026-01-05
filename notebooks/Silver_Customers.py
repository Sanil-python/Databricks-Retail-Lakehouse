# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@retailsalesanalysissanil.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df = df.withColumn("domains", split(col("email"), "@")[1])
df.display()

# COMMAND ----------

df.groupBy("domains").agg(count("*").alias("total_count")).orderBy(col("total_count").desc()).display()

# COMMAND ----------

df = df.withColumn("full_name", concat(col("first_name"), lit(' '), col("last_name")))
df = df.drop("first_name", "last_name")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
  .mode("overwrite")\
  .save("abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table Creation to top of the Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists rsa_cata.silver.customers_silver 
# MAGIC using delta
# MAGIC location 'abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/customers'