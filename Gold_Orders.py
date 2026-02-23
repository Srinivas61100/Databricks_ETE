# Databricks notebook source
# MAGIC %md
# MAGIC ## **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_catlog.silver.orders_silver")
df.display()

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from databricks_catlog.gold.dimcustomers")

df_dipro = spark.sql("select product_id as dim_product_id, product_id as DimProductKey from databricks_catlog.gold.dimproducts")

# COMMAND ----------

df_fact = df.join(df_dimcus, df.customer_id == df_dimcus.dim_customer_id, "left").join(df_dipro, df.product_id == df_dipro.dim_product_id, "left")

df_fact_new = df_fact.drop("dim_customer_id", "dim_product_id", "customer_id", "product_id")

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catlog.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_catlog.gold.FactOrders")
    
    dlt_obj.alias("tgt").merge(df_fact_new.alias("src"), "tgt.order_id = src.order_id AND tgt.DimCustomerKey = src.DimCustomerKey AND tgt.DimProductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_fact_new.write.format("delta")\
            .option("path", "abfss://gold@databricksetl611.dfs.core.windows.net/FactOrders")\
            .saveAsTable("databricks_catlog.gold.FactOrders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_catlog.gold.factorders

# COMMAND ----------

