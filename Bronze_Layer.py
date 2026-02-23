# Databricks notebook source
# MAGIC %md
# MAGIC ## **Dynamic Capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.SchemaLocation",f"abfss://bronze@databricksetl611.dfs.core.windows.net/checkPoint_{file_name}")\
                .load(f"abfss://source@databricksetl611.dfs.core.windows.net/{file_name}")
                

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Writing**

# COMMAND ----------

df.writeStream.format("parquet")\
        .outputMode("append")\
        .option("checkpointLocation",f"abfss://bronze@databricksetl611.dfs.core.windows.net/checkPoint_{file_name}")\
        .option("path",f"abfss://bronze@databricksetl611.dfs.core.windows.net/{file_name}")\
        .trigger(once = True)\
        .start()

# COMMAND ----------

df = spark.read.format("parquet")\
            .load(f"abfss://bronze@databricksetl611.dfs.core.windows.net/{file_name}")
display(df)