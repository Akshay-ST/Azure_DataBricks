# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/user/hive/warehouse/retail_db.db/

# COMMAND ----------

# MAGIC %sql
# MAGIC use database retail_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table retail_db.orders (
# MAGIC   order_id int,
# MAGIC   order_date string,
# MAGIC   customer_id int,
# MAGIC   order_status string
# MAGIC )
# MAGIC using delta
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC data for above table will be stored in:<br>
# MAGIC /user/hive/warehouse/retail_db.db/orders <br>
# MAGIC <br>
# MAGIC For alredy created table, enabling CDF: <br>
# MAGIC ALTER TABLE [table_name] <br>
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)<br>
# MAGIC <br>
# MAGIC For all tables in future to have Change data feed enabled by default, set : - <br>
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders values 
# MAGIC (1,'2013-07-2500:00:00.0',11599,'CLOSED'),
# MAGIC (2,'2013-07-2500:00:00.0',256,'PENDING_PAYMENT'),
# MAGIC (3,'2013-07-2500:00:00.0',12111,'COMPLETE'),
# MAGIC (4,'2013-07-2500:00:00.0',8827,'CLOSED'),
# MAGIC (5,'2013-07-2500:00:00.0',11318,'COMPLETE');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',1);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from orders where order_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',2);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',1);

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE orders
# MAGIC SET order_status = 'COMPLETE'
# MAGIC WHERE order_id = 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('orders',3);

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE retail_db.orders_bronze (
# MAGIC     order_id int,
# MAGIC     order_date string,
# MAGIC     customer_id int,
# MAGIC     order_status string,
# MAGIC     filename string,
# MAGIC     createdOn timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/files/medallion/orders_bronze.delta'
# MAGIC PARTITIONED BY (order_status)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE retail_db.orders_silver (
# MAGIC     order_id int,
# MAGIC     order_date date,
# MAGIC     customer_id int,
# MAGIC     order_status string,
# MAGIC     order_year int GENERATED ALWAYS AS (YEAR(order_date)),
# MAGIC     order_month int GENERATED ALWAYS AS (MONTH(order_date)),
# MAGIC     createdOn TIMESTAMP,
# MAGIC     modifiedOn TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/files/medallion/orders_silver.delta'
# MAGIC PARTITIONED BY (order_status)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE retail_db.orders_gold (
# MAGIC     customer_id int,
# MAGIC     order_status string,
# MAGIC     order_year int,
# MAGIC     num_orders int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/files/medallion/orders_gold.delta'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------


