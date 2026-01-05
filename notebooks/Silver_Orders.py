# Databricks notebook source
# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
  .load("abfss://bronze@retailsalesanalysissanil.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df = df.withColumn("year", year(col("order_date")))
df.display()

# COMMAND ----------

window_spec = Window.partitionBy("year").orderBy(col("total_amount").desc())
df1 = df.withColumn("dense_flag", dense_rank().over(window_spec))
df1.display()

# COMMAND ----------

df1 = df.withColumn("rank_flag", rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
df1.display()

# COMMAND ----------

df1 = df.withColumn("row_flag", row_number().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Classes - OOP**

# COMMAND ----------

class Windows:
  def dense_rank(self, df):
    df_dense_rank = df.withColumn("dense_flag", dense_rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
    return df_dense_rank
  
  def rank(self, df):
    df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy(col("year")).orderBy(col("total_amount").desc())))
    return df_rank
  
  def row_number(self, df):
    df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy(col("year")).orderBy(col("total_number").desc())))
    return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = Windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta")\
  .mode("overwrite")\
  .save("abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table Creation to top of the Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists rsa_cata.silver.orders_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/orders'