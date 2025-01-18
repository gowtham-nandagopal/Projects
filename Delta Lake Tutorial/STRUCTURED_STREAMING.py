# Databricks notebook source
# MAGIC %sql
# MAGIC Create table delta_catalog.raw.external_streaming_src
# MAGIC (
# MAGIC   id int,
# MAGIC   order_name STRING,
# MAGIC   amount int,
# MAGIC   product_id int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://raw@pysparklearningadls.dfs.core.windows.net/streaming_src'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.external_streaming_src
# MAGIC Values 
# MAGIC (1, 'biscuits', 10, 101),
# MAGIC (2, 'noodles', 10, 102),
# MAGIC (3, 'biscuits', 20, 103)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_streaming_src;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Query

# COMMAND ----------

df = spark.readStream.table("delta_catalog.raw.external_streaming_src")

# COMMAND ----------

df.writeStream.format("delta") \
    .option("checkpointLocation", "abfss://raw@pysparklearningadls.dfs.core.windows.net/streaming/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start("abfss://raw@pysparklearningadls.dfs.core.windows.net/streaming/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@pysparklearningadls.dfs.core.windows.net/streaming/data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.external_streaming_src
# MAGIC Values 
# MAGIC (4, 'Chocolates', 10, 104),
# MAGIC (5, 'toys', 10, 105),
# MAGIC (6, 'bedspreads', 20, 106)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@pysparklearningadls.dfs.core.windows.net/streaming/data`;

# COMMAND ----------


