# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating a Schema Under Delta Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC Create schema delta_catalog.raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Delta Table (Managed)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table delta_catalog.raw.managed_table_orders
# MAGIC (
# MAGIC   id int,
# MAGIC   order_name STRING,
# MAGIC   amount int,
# MAGIC   product_id int
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.managed_table_orders
# MAGIC Values 
# MAGIC (1, 'biscuits', 10, 101),
# MAGIC (2, 'noodles', 10, 102),
# MAGIC (3, 'biscuits', 20, 103)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe detail delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Delta Table (External)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table delta_catalog.raw.external_table_orders
# MAGIC (
# MAGIC   id int,
# MAGIC   order_name STRING,
# MAGIC   amount int,
# MAGIC   product_id int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://raw@pysparklearningadls.dfs.core.windows.net/external_tables'

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
# MAGIC DESCRIBE EXTENDED delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://raw@pysparklearningadls.dfs.core.windows.net/external_tables`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an External Table Using CETAS

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table delta_catalog.raw.external_table_cetas
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://raw@pysparklearningadls.dfs.core.windows.net/external_tables_cetas'
# MAGIC AS
# MAGIC Select * from delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_cetas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cloning in Delta Lake
# MAGIC **Deep Clone**
# MAGIC - Copies the delta log and actual files to the target location
# MAGIC - As of now there is no option to create external deep clone table. We can have only managed deep clone tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw.external_table_deep_clone
# MAGIC deep clone delta_catalog.raw.external_table_orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Shallow CLone**
# MAGIC - Shallow clone is possible with the managed tables and not external tables
# MAGIC - Copies only the delta log and not actual files

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta_catalog.raw.external_table_shallow_clone
# MAGIC shallow clone delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_shallow_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.managed_table_orders
# MAGIC Values 
# MAGIC (4, 'chips', 15, 105)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_shallow_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta_catalog.raw.managed_table_orders where product_id = 103

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_shallow_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.managed_table_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH FOREIGN TABLE delta_catalog.raw.external_table_shallow_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_shallow_clone
