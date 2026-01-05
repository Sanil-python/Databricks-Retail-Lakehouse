# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("initial_load_flag", "0")

# COMMAND ----------

initial_load_flag = int(dbutils.widgets.get("initial_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading from Source Table (Silver Layer)**

# COMMAND ----------

df = spark.sql(
  "select * from rsa_cata.silver.customers_silver"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing Duplicates**

# COMMAND ----------

df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dividing New vs Old Records**

# COMMAND ----------

# MAGIC %md
# MAGIC -- Here in the else clause, I have considered 0 as all the column values in the else clause because I only want the table structure.

# COMMAND ----------

if initial_load_flag == 0:
  df_old = spark.sql(
    '''select DimCustomerKey, customer_id, create_date, update_date
       from rsa_cata.gold.DimCustomers'''
  )

else:
  df_old = spark.sql(
    '''select 0 as DimCustomerKey, 0 as customer_id, 0 as create_date, 0 as update_date
       from rsa_cata.silver.customers_silver where 1=0'''
  )

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Columns of df_old for better column management**

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
    .withColumnRenamed("customer_id", "old_customer_id")\
    .withColumnRenamed("create_date", "old_create_date")\
    .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Join with the Old Records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')


# COMMAND ----------

df_join.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Separating Old vs New Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())
df_new.limit(5).display()

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimcustomerKey'].isNotNull())
df_old.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# Dropping the Unneccessary columns
#Here, I am dropping the old_update_date column and not the old_create_date column because create date will always be same but update date will change.
df_old = df_old.drop("old_customer_id", "old_update_date")

# Renaming "old_DimCustomerKey" to "DimCustomerKey"
df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# Renaming "old_create_date" to "create_date"
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn('create_date', to_timestamp(col('create_date')))

# Recreating "update_date" column with current timestamp
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

# Dropping the Unneccessary columns
df_new = df_new.drop("old_DimCustomerKey", "old_customer_id", "old_create_date", "old_update_date")

# Recreating "update_date", "current_date" column with current timestamp
df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Key - Starting from 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))

# COMMAND ----------

df_new.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding Max Surrogate Key for Incremental Load**

# COMMAND ----------

if initial_load_flag == 1:
    max_surrogate_key = 0
else:
    df_max_sur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from rsa_cata.gold.DimCustomers")
    # Converting df_max_sur to max_surrogate_key variable
    max_surrogate_key = df_max_sur.collect()[0]['max_surrogate_key']

# COMMAND ----------

# Adding the max_surrogate_key to the df_new

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Union of df_old & df_new**

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing Duplicate Customer Keys for smooth MERGE operation**

# COMMAND ----------

df_dedup = (
    df_final
    .withColumn("rn", row_number().over(Window.partitionBy("DimCustomerKey").orderBy(col("update_date").desc())))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD - 1**

# COMMAND ----------

# Using the MERGE condition

if spark.catalog.tableExists("rsa_cata.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@retailsalesanalysissanil.dfs.core.windows.net/DimCustomers")
    dlt_obj.alias("trg").merge(df_dedup.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@retailsalesanalysissanil.dfs.core.windows.net/DimCustomers")\
        .saveAsTable("rsa_cata.gold.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rsa_cata.gold.dimcustomers