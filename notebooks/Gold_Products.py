# Databricks notebook source
# MAGIC %md
# MAGIC # **DLT Pipeline**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

# Expectations
my_expectations = {
    "rule1" : "product_id is not null",
    "rule2" : "product_name is not null"
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_expectations)

def DimProducts_stage():
    df = spark.readStream.table('rsa_cata.silver.products_silver')
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Steaming View on Top of the Streaming Table**

# COMMAND ----------

@dlt.view
def DimProducts_view():
  df = spark.readStream.table("LIVE.DimProducts_stage")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
    target = "DimProducts",
    source = "LIVE.DimProducts_view",
    keys = ["product_id"],
    sequence_by = "product_id",
    stored_as_scd_type = 2
)

# COMMAND ----------

