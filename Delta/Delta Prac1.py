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

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting into DELTA TABLE<br>
# MAGIC 1. Insert command<br>
# MAGIC 2. Append<br>
# MAGIC 3. Copy command<br>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_delta 
# MAGIC where order_status = 'CANCELED' 
# MAGIC   and date(order_date) < '2013-07-28';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into retail_db.orders_delta values ('11111111','2013-07-25 00:00:00.0','22222222','CLOSED')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_delta 
# MAGIC where order_status = 'CLOSED' 
# MAGIC   and date(order_date) < '2013-07-26';

# COMMAND ----------

dfnew = spark.read.csv('/mnt/files/ordersappend.csv', header = True)

dfnew.write.mode("append") \
  .partitionBy("order_status") \
  .format("delta") \
  .save("/mnt/files/delta/orders.delta")




# COMMAND ----------

dfnew.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into retail_db.orders_delta 
# MAGIC from '/mnt/files/orders1.csv' 
# MAGIC fileformat = csv 
# MAGIC format_options('header'='true')
# MAGIC --COPY_OPTIONS ('mergeSchema'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into retail_db.orders_delta 
# MAGIC from '/mnt/files/orders_4R_5C.csv' 
# MAGIC fileformat = csv 
# MAGIC format_options('header'='true')
# MAGIC --COPY_OPTIONS ('mergeSchema'='true')

# COMMAND ----------

df_new = spark.read.csv('/mnt/files/orders_4R_5C.csv', header = True)
df_new.show()


# COMMAND ----------

df_new.write.mode("append") \
  .partitionBy("order_status") \
  .format("delta") \
  .option("mergeSchema","true") \
  .save("/mnt/files/delta/orders.delta")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_db.orders_delta
# MAGIC where order_amount is not null
# MAGIC    --or date(order_date) = '2013-07-25';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC update retail_db.orders_delta 
# MAGIC set order_status = 'CLOSED' 
# MAGIC where order_id = 2222222

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_delta

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets

# COMMAND ----------


