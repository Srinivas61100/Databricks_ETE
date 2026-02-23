# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_catlog.silver.customers_silver")

# COMMAND ----------

# Removing Duplicates

df = df.dropDuplicates(subset = ["customer_id"])

# COMMAND ----------

# Dividing New vs Old Records

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date
                       from databricks_catlog.gold.DimCustomers''')
else:
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date
                       from databricks_catlog.silver.customers_silver
                       where 1=0''')

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
              .withColumnRenamed("customer_id", "old_customer_id")\
              .withColumnRenamed("create_date", "old_create_date")\
              .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, "left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# Seperating New vs Old Records

df_new = df_join.filter(df_join.old_DimCustomerKey.isNull())

# COMMAND ----------

df_old = df_join.filter(df_join.old_DimCustomerKey.isNotNull())

# COMMAND ----------

# Preparing df_old

# Dropping all the columns which are not required
df_old = df_old.drop("old_customer_id", "old_update_date")


# Renaming old_DimCustomerKey column to DimCustomerKey
df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# Renaming old_create_date column to create_date
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# Recreating update_date column with the current timestamp
df_old = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# Preparing df_new

# Dropping all the columns which are not required
df_new = df_new.drop("old_DimCustomerKey", "old_customer_id", "old_update_date", "old_create_date")

# Recreating update_date, current_date columns with the current timestamp
df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# Surrogate Key - From 1

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+1)
df_new.display()

# COMMAND ----------

# Adding Max Surrogate Key

if init_load_flag == 1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_catlog.gold.DimCustomers")
    # Converting df_maxsur to max_surrogate_key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", df_new.DimCustomerKey + max_surrogate_key)

# COMMAND ----------

# Union of df_old and df_new

df_final = df_new.unionByName(df_old)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD TYPE - 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catlog.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databricksetl611.dfs.core.windows.net/dimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    
else:
    df_final.write.mode("overwrite")\
        .format("delta")\
        .option("path", "abfss://gold@databricksetl611.dfs.core.windows.net/dimCustomers")\
        .saveAsTable("databricks_catlog.gold.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_catlog.gold.dimcustomers

# COMMAND ----------

