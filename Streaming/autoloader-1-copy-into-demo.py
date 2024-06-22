# Databricks notebook source
# MAGIC %sql
# MAGIC create database autoloader;
# MAGIC create table if not exists autoloader.orders(
# MAGIC   order_id int,
# MAGIC   order_date string,
# MAGIC   customer_id int,
# MAGIC   order_status string
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe table autoloader.orders;

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe detail autoloader.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC use database autoloader;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/files

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into orders
# MAGIC from (select order_id::int, order_date, customer_id::int, order_status 
# MAGIC       from 'dbfs:/mnt/files/ordersappend.csv')
# MAGIC fileformat = CSV
# MAGIC format_options('header'='true')
