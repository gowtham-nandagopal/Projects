# Databricks notebook source
# MAGIC %md
# MAGIC ## Optimize
# MAGIC - This compacts small files into larger ones, reducing the number of files scanned during queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.external_table_orders
# MAGIC Values 
# MAGIC (1, 'biscuits', 10, 101),
# MAGIC (2, 'noodles', 10, 102),
# MAGIC (3, 'biscuits', 20, 103)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta_catalog.raw.external_table_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZORDERBY
# MAGIC - It does NOT sort the data in the files in a strict order like ORDER BY. Instead, it rearranges data within files to improve query performance. 
# MAGIC - Clusters similar values together within the same files.
# MAGIC - This might be useful when we have filtering condition querying the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta_catalog.raw.external_table_orders ZORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## LIQUID CLUSTERING
# MAGIC - Distributes data evenly across files instead of creating separate folders.
# MAGIC - Best for high-cardinality columns (e.g., customer_id, transaction_id).
# MAGIC - Helps with reducing data skew and improving query performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TO enable liquid clustering
# MAGIC ALTER TABLE table_name
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.liquidClustering.enabled' = 'true',
# MAGIC   'delta.liquidClustering.columns' = 'customer_id, order_date'
# MAGIC );

# COMMAND ----------


