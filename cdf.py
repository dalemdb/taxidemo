# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('tripdata_gold',0)

# COMMAND ----------

cdf_df = (
  spark
    .readStream
    .option("readChangeData", True)
    .option("startingVersion", 0)
    .table("tripdata_gold")
)
display(cdf_df)
