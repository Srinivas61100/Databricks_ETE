# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
            .load("abfss://bronze@databricksetl611.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Functions**

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catlog.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE 
# MAGIC LANGUAGE SQL
# MAGIC RETURN ROUND(p_price * 0.90, 2) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, price, databricks_catlog.bronze.discount_func(price) as discounted_price
# MAGIC FROM products

# COMMAND ----------

df = df.withColumn("discounted_price", expr("databricks_catlog.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catlog.bronze.upper_func(brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS
# MAGIC $$
# MAGIC   return brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_id, brand, databricks_catlog.bronze.upper_func(brand) as brand_upper
# MAGIC FROM products

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetl611.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catlog.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetl611.dfs.core.windows.net/products'

# COMMAND ----------

