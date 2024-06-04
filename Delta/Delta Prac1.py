# Databricks notebook source
dbutils.fs.help('unmount')

# COMMAND ----------

dbutils.fs.unmount('/mnt/files/')

# COMMAND ----------

storageAccountName = 'retailanalyticssa'
blobContainerName = 'prac'
storageAccountAccessKey = dbutils.secrets.get(scope = "sa_key", key = "sakey")

dbutils.fs.mount(
  source = f'wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net',
  mount_point = '/mnt/files/',
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/files

# COMMAND ----------

df = spark.read.csv('/mnt/files/orders.csv', header = True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite") \
  .partitionBy("order_status") \
  .format("parquet") \
  .save("/mnt/files/parquet/orders.parquet")


# COMMAND ----------

df.write.mode("overwrite") \
  .partitionBy("order_status") \
  .format("delta") \
  .save("/mnt/files/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exist retail_db;

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table retail_db.orders_parquet 
# MAGIC USING PARQUET LOCATION '/mnt/files/parquet/orders.parquet'

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table retail_db.orders_delta 
# MAGIC USING DELTA LOCATION '/mnt/files/delta/orders.delta'
