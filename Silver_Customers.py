# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##**Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@databricksetl611.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df = df.withColumn("domains", split(col("email"), "@")[1])
df.display()

# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers", ascending=False).limit(3).display()

# COMMAND ----------

df_gmail = df.filter(col("domains") == "gmail.com")
df_yahoo = df.filter(col("domains") == "yahoo.com")
df_hotmail = df.filter(col("domains") == "hotmail.com")
display(
    df_gmail.select(
        "customer_id", 
        concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"), 
        "email"
    )
)

# COMMAND ----------

df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetl611.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_catlog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catlog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetl611.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_catlog.silver.customers_silver

# COMMAND ----------

