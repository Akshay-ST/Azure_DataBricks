# Databricks notebook source
# MAGIC %fs 
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow

# COMMAND ----------

# MAGIC %fs 
# MAGIC head dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz

# COMMAND ----------

trip_df = spark.read.csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz", header=True)

# COMMAND ----------

trip_df = spark.read \
        .format("csv") \
        .option("header","true") \
        .option("inferSchema","true") \
        .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz")

# COMMAND ----------

trip_df.show(truncate=False)

# COMMAND ----------

display(trip_df)

# COMMAND ----------

trip_df.count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database trip_db;

# COMMAND ----------

trip_df.repartition(20) \
    .write.format("delta") \
    .saveAsTable("trip_db.trips_delta")



# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail trip_db.trips_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended trip_db.trips_delta

# COMMAND ----------

trip_df.repartition(20) \
    .write.format("parquet") \
    .saveAsTable("trip_db.trips_parquet")



# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail trip_db.trips_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended trip_db.trips_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(Fare_Amt), max(Fare_Amt)
# MAGIC from trip_db.trips_delta;
# MAGIC
# MAGIC -- 1.03 sec

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(Fare_Amt), max(Fare_Amt)
# MAGIC from trip_db.trips_parquet;
# MAGIC
# MAGIC -- 10.62 sec

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from trip_db.trips_delta;
# MAGIC
# MAGIC -- 0.82 sec

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from trip_db.trips_parquet;
# MAGIC
# MAGIC -- 1.15 sec

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from trip_db.trips_delta
# MAGIC where Total_Amt = 234
# MAGIC
# MAGIC -- 1.36

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from trip_db.trips_parquet
# MAGIC where Total_Amt = 234
# MAGIC
# MAGIC -- 1.67
# MAGIC
