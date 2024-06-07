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

# MAGIC %fs
# MAGIC ls dbfs:/mnt/files/

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO retail_db.orders_bronze FROM (
# MAGIC   SELECT 
# MAGIC     order_id::int,
# MAGIC     order_date::string,
# MAGIC     customer_id::int,
# MAGIC     order_status::string,
# MAGIC     INPUT_FILE_NAME() as filename,
# MAGIC     CURRENT_TIMESTAMP as createdOn
# MAGIC   FROM 'dbfs:/mnt/files/orders1.csv'
# MAGIC )
# MAGIC fileformat = CSV
# MAGIC format_options('header' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC re running the above command won't insert the data twice, COPT command is smart enough

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_bronze limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history retail_db.orders_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended retail_db.orders_bronze;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes("retail_db.orders_bronze", 1) limit 7;

# COMMAND ----------

# MAGIC %md
# MAGIC Next Step is to take changes in bronze table and merge it to Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_bronze_Changes as 
# MAGIC select * from table_changes("retail_db.orders_bronze", 1)
# MAGIC where order_id > 0 and customer_id > 0
# MAGIC and order_status in ("PAYMENT_REVIEW","PROCESSING","CLOSED","SUSPECTED_FRAUD","PENDING","CANCELLED","PENDING_PAYMENT","COMPLETE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze_changes;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc retail_db.orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_db.orders_silver tgt
# MAGIC USING orders_bronze_changes src ON tgt.order_id = src.order_id
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE
# MAGIC   SET tgt.order_status = src.order_status,
# MAGIC       tgt.customer_id = src.customer_id,
# MAGIC       tgt.modifiedOn = CURRENT_TIMESTAMP()
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (order_id,order_date,customer_id,order_status, createdOn, modifiedOn)
# MAGIC      VALUES (order_id, order_date, customer_id, order_status, CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_db.orders_silver limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table retail_db.orders_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE retail_db.orders_gold
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     order_year,
# MAGIC     count(order_id) as num_orders
# MAGIC   FROM retail_db.orders_silver
# MAGIC   GROUP BY all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from retail_db.orders_gold
# MAGIC order by 1,2,3;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC managing the new file coming in

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO retail_db.orders_bronze FROM (
# MAGIC   SELECT 
# MAGIC     order_id::int,
# MAGIC     order_date::string,
# MAGIC     customer_id::int,
# MAGIC     order_status::string,
# MAGIC     INPUT_FILE_NAME() as filename,
# MAGIC     CURRENT_TIMESTAMP as createdOn
# MAGIC   FROM 'dbfs:/mnt/files/orders_4R_new.csv'
# MAGIC )
# MAGIC fileformat = CSV
# MAGIC format_options('header' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_bronze_Changes as 
# MAGIC select * from table_changes("retail_db.orders_bronze", 2)
# MAGIC where order_id > 0 and customer_id > 0
# MAGIC and order_status in ("PAYMENT_REVIEW","PROCESSING","CLOSED","SUSPECTED_FRAUD","PENDING","CANCELLED","PENDING_PAYMENT","COMPLETE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_bronze_changes;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_db.orders_silver tgt
# MAGIC USING orders_bronze_changes src ON tgt.order_id = src.order_id
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE
# MAGIC   SET tgt.order_status = src.order_status,
# MAGIC       tgt.customer_id = src.customer_id,
# MAGIC       tgt.modifiedOn = CURRENT_TIMESTAMP()
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (order_id,order_date,customer_id,order_status, createdOn, modifiedOn)
# MAGIC      VALUES (order_id, order_date, customer_id, order_status, CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_db.orders_gold;
# MAGIC -- 48,510

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE retail_db.orders_gold
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     order_year,
# MAGIC     count(order_id) as num_orders
# MAGIC   FROM retail_db.orders_silver
# MAGIC   GROUP BY all;
