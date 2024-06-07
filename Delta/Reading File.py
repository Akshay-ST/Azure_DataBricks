# Databricks notebook source
#secret/createScope
dbutils.secrets.list('sa_key')

# COMMAND ----------

dbutils.fs.unmount('/mnt/files2/')

# COMMAND ----------

storageAccountName = 'retailanalyticssa'
blobContainerName = 'prac'
storageAccountAccessKey = dbutils.secrets.get(scope = "sa_key", key = "sakey")

dbutils.fs.mount(
  source = f'wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net',
  mount_point = '/mnt/files2/',
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
)

# COMMAND ----------

# MAGIC %fs ls '/mnt/files2/'

# COMMAND ----------

# MAGIC %fs
# MAGIC head /mnt/files/orders.csv

# COMMAND ----------

df = spark.read.csv('/mnt/files/orders.csv', header=True)


# COMMAND ----------

df.show(5)

# COMMAND ----------

df.write.mode("overwrite") \
  .format("delta") \
  .option("header",'true') \
  .option("delta.columnMapping.mode","name") \
  .save('dbfs:/mnt/files2/nb0/orders_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/files2/nb0/orders_delta` limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS retail_db2;
# MAGIC CREATE TABLE IF NOT EXISTS retail_db2.orders_delta
# MAGIC USING DELTA LOCATION 'dbfs:/user/hive/warehouse/retail_db2.db' AS
# MAGIC   select * from delta.`dbfs:/mnt/files2/nb0/orders_delta`;
# MAGIC
# MAGIC ALTER TABLE retail_db2.orders_delta SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db2.orders_delta limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_db2.orders_delta
# MAGIC select * from delta.`dbfs:/mnt/files2/delta`;
