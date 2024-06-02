# Databricks notebook source
storageAccountName = 'retailanalyticssa'
blobContainerName = 'input'
storageAccountAccessKey = dbutils.secrets.get(scope = "sa_key", key = "sakey")

dbutils.fs.mount(
  source = f'wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net',
  mount_point = '/mnt/files/',
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
)

# COMMAND ----------

# MAGIC %fs ls '/mnt/files/'

# COMMAND ----------

df = spark.read.option("delimiter",",").option("header",True).csv('/mnt/files/news_headlines_20_days.csv')

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("header",True).option("delta.columnMapping.mode","name").save('dbfs:/Azure_ast/news_file')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/Azure_ast/news_file`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS DEV_DB.news_file1
# MAGIC
# MAGIC USING DELTA LOCATION 'dbfs:/Azure_ast/news_file1' AS
# MAGIC
# MAGIC select * from delta.`dbfs:/Azure_ast/news_file`;
# MAGIC
# MAGIC ALTER TABLE DEV_DB.news_file1 SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DEV_DB.news_file1 VALUES
# MAGIC select * from delta."dbfs:/Azure_ast/news_file";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DEV_DB.news_file1;
