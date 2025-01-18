# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Streaming Table

# COMMAND ----------

@dlt.table(
    name = "bronze_tbl"
)
def bronze_tbl_src():
    df = spark.readStream.table("delta_catalog.raw.external_streaming_src")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver View

# COMMAND ----------

@dlt.view

def silver_view():
    df = spark.read.table("LIVE.bronze_tbl")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Materialized View

# COMMAND ----------

@dlt.table

def gold_materialized_vw():
    df = spark.read.table("LIVE.silver_view")
    return df

# COMMAND ----------


