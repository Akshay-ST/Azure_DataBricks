# Databricks notebook source
# MAGIC %fs 
# MAGIC ls dbfs:/mnt/files/data_json_orders/

# COMMAND ----------

landing_zone = "dbfs:/mnt/files/data_json_orders"
orders_data = landing_zone + "/orders_data"
checkPoint_path = landing_zone + "/orders_checkpoints"

# COMMAND ----------

orders_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.inferSchema", "true") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaLocation", orders_data + "/schema") \
    .load(orders_data)
    

# COMMAND ----------

orders_df.display()

# COMMAND ----------

orders_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkPoint_path) \
    .outputMode("append") \
    .toTable("autoloader.orderdelta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader.orderdelta;

# COMMAND ----------


