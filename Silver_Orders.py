# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@databricksetl611.dfs.core.windows.net/orders")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df.display()

# COMMAND ----------

df = df.withColumn("year", year(col("order_date")))
df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1 = df1.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1 = df1.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Classes - OOPS**

# COMMAND ----------

class windows:
    
    def dense_rank(self, df):
        df_dense_rank = df.withColumn("dense_rank", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_dense_rank
    
    def rank(self, df):
        df_rank = df.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank
    
    def row_number(self, df):
        df_row_number = df.withColumn("row_number", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_row_number



# COMMAND ----------

df_new = df
df_new.display()

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databricksetl611.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catlog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksetl611.dfs.core.windows.net/orders'