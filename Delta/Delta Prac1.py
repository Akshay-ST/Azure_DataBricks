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
# MAGIC USING PARQUET LOCATION '/mnt/files/parquet/orders.parquet/o*'

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table retail_db.orders_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_parquet limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retail_db.orders_parquet

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table retail_db.orders_delta 
# MAGIC USING DELTA LOCATION '/mnt/files/delta/orders.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_delta limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended retail_db.orders_delta

# COMMAND ----------

# MAGIC %md
# MAGIC combining 2 steps into one <br>
# MAGIC step one - write data from DF <br>
# MAGIC step two - create a table <br>
# MAGIC Below statements represent these steps

# COMMAND ----------

df = spark.read.csv('/mnt/files/orders.csv', header = True)

# COMMAND ----------

#step 1
df.write.mode("overwrite") \
  .partitionBy("order_status") \
  .format("delta") \
  .save("/mnt/files/delta/orders.delta")

# COMMAND ----------

#step 2
%sql 
create table retail_db.orders_delta 
USING DELTA LOCATION '/mnt/files/delta/orders.delta'

# COMMAND ----------

# MAGIC %md
# MAGIC Now, We will combine these steps below

# COMMAND ----------

df.write \
.mode("overwrite") \
.partitionBy("order_status") \
.format("delta") \
.option("path",'/mnt/files/delta2/orders.delta') \
.saveAsTable("retail_db.oders_delta2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.oders_delta2 where order_Status = 'COMPLETE' limit 10;
