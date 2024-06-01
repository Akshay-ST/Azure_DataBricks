# Databricks notebook source
# MAGIC %python
# MAGIC storageAccountName = 'ast1storage'
# MAGIC blobContainerName = 'container1'
# MAGIC storageAccountAccessKey = 'GSThIGU4jThdtVxIdhuV2P0+0A3ZFv5ZmoLqQ2WhBXxhO7kFWauh7tIXRHA82olRJVacdArviZe0+AStczkIIw=='
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC   source = f'wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net',
# MAGIC   mount_point = '/mnt/files/',
# MAGIC   extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
# MAGIC )
# MAGIC

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
