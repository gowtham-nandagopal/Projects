# Databricks notebook source
# MAGIC %md
# MAGIC **Merge Schema**

# COMMAND ----------

data = [(1, 'order1', 100, 1), (2, 'order2', 200, 2), (3, 'order3', 300, 3), (4, 'order4', 400, 4)]
schema = "id INT, order_name STRING, amount INT, product_id INT"

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path", "abfss://raw@pysparklearningadls.dfs.core.windows.net/schema_evolution").save()

# COMMAND ----------

new_order = df.union(spark.createDataFrame([(5, 'order5', 500, 5)], schema))

# COMMAND ----------

new_order.display()

# COMMAND ----------

new_order.write.format("delta").mode("overwrite").option("path", "abfss://raw@pysparklearningadls.dfs.core.windows.net/schema_evolution").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@pysparklearningadls.dfs.core.windows.net/schema_evolution`

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

new_order = new_order.withColumn("date", F.lit("2022-01-01"))

# COMMAND ----------

new_order.display()

# COMMAND ----------

new_order.write.format("delta").mode("overwrite").option("path", "abfss://raw@pysparklearningadls.dfs.core.windows.net/schema_evolution").option("mergeSchema", True).save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@pysparklearningadls.dfs.core.windows.net/schema_evolution`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicit Schema Update

# COMMAND ----------

# MAGIC %md
# MAGIC **Add a Column**

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER Table delta_catalog.raw.external_table_orders ADD COLUMN Date date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC **ADD a Column After Specific Column**

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER table delta_catalog.raw.external_table_orders ADD COLUMN discount double after amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC **REORDERING Columns**

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER table delta_catalog.raw.external_table_orders ALTER COLUMN discount AFTER product_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Column**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If your table is already on the required protocol version:
# MAGIC ALTER TABLE delta_catalog.raw.external_table_orders 
# MAGIC SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC -- Now you can rename the column
# MAGIC ALTER TABLE delta_catalog.raw.external_table_orders 
# MAGIC RENAME COLUMN Date TO dt;

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## REORG
# MAGIC - REORG is used to reorganize Delta table data for better performance.
# MAGIC - It mainly focuses on small file compaction and clustering data more efficiently.
# MAGIC - Similar to OPTIMIZE, but with more flexibility and better handling of skewed data.

# COMMAND ----------

# MAGIC %sql
# MAGIC REORG table delta_catalog.raw.external_table_orders Apply (PURGE);

# COMMAND ----------


