# Databricks notebook source
# MAGIC %md
# MAGIC ### **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA READING**

# COMMAND ----------

df = spark.sql("select * from rsa_cata.silver.orders_silver")
df.display()

# COMMAND ----------

df_dim_cus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from rsa_cata.gold.dimcustomers")

# COMMAND ----------

df_dim_pro = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from rsa_cata.gold.dimproducts")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact Dataframe**

# COMMAND ----------

df_fact = df.join(df_dim_cus, df.customer_id == df_dim_cus.dim_customer_id, 'left').join(df_dim_pro, df.product_id == df_dim_pro.dim_product_id, 'left')

df_fact_new = df_fact.drop('dim_customer_id', 'dim_product_id', 'customer_id', 'product_id')

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying UPSERT on Fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("rsa_cata.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "rsa_cata.gold.FactOrders")
    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id and trg.DimCustomerKey = src.DimCustomerKey and trg.DimproductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact_new.write.format("delta")\
        .option("path", "abfss://gold@retailsalesanalysissanil.dfs.core.windows.net/FactOrders")\
        .saveAsTable("rsa_cata.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rsa_cata.gold.factorders