# Databricks notebook source
# MAGIC %md
# MAGIC ## DML With Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table delta_catalog.raw.external_table_orders_dml_dv
# MAGIC (
# MAGIC   id int,
# MAGIC   order_name STRING,
# MAGIC   amount int,
# MAGIC   product_id int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://raw@pysparklearningadls.dfs.core.windows.net/external_tables_dml_dv'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into delta_catalog.raw.external_table_orders_dml_dv
# MAGIC Values 
# MAGIC (1, 'biscuits', 10, 101),
# MAGIC (2, 'noodles', 10, 102),
# MAGIC (3, 'biscuits', 20, 103)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders_dml_dv

# COMMAND ----------

# MAGIC %md
# MAGIC **Enabling Deletion Vectors**

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter table delta_catalog.raw.external_table_orders_dml_dv set tblproperties ('delta.enableDeletionVectors' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC **Update Delta Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC Update delta_catalog.raw.external_table_orders_dml_dv set amount = 30 where id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_catalog.raw.external_table_orders_dml_dv

# COMMAND ----------


