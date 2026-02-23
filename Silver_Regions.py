# Databricks notebook source
df = spark.read.table("databricks_catlog.bronze.regions")

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetl611.dfs.core.windows.net/regions")

# COMMAND ----------

df_read = spark.read.format("delta").load("abfss://silver@databricksetl611.dfs.core.windows.net/regions")
df_read.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catlog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetl611.dfs.core.windows.net/regions'

# COMMAND ----------

