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
