# Databricks notebook source
df = spark.read.table("rsa_cata.bronze.regions")
df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/regions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Table Creation to top of the Data**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists rsa_cata.silver.regions_silver
# MAGIC using delta
# MAGIC location 'abfss://silver@retailsalesanalysissanil.dfs.core.windows.net/regions'