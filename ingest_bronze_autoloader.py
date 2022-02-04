# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ### ingest_bronze_autoloader notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create AutoLoader stream source

# COMMAND ----------

yellow_auto_schema = """
  VendorID integer,
  tpep_pickup_datetime string,
  tpep_dropoff_datetime string,
  passenger_count integer,
  trip_distance double,
  RatecodeID integer,
  store_and_fwd_flag string,
  PULocationID integer,
  DOLocationID integer,
  payment_type integer,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double
"""

yellow_auto_df = (
  spark.readStream.format("cloudFiles") 
    .schema(yellow_auto_schema)
    .option("cloudFiles.format", "json")        
    .option("cloudFiles.resourceGroup", resource_group_name)
    .option("cloudFiles.subscriptionId", subscription_id)
    .option("cloudFiles.tenantId", tenant_id)
    .option("cloudFiles.clientId", client_id)
    .option("cloudFiles.clientSecret", client_secret)
    .option("cloudFiles.includeExistingFiles", True)
    .option("cloudFiles.maxFilesPerTrigger", 9600)    
    .option("cloudFiles.useNotifications", True) 
    .load(f"wasbs://ingest@{blob_name}/drop/*.json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader stream into Tripdata Bronze Delta Lake table sink

# COMMAND ----------

from pyspark.sql.functions import lit, col, expr
(
  yellow_auto_df
    .withColumn("id", expr("uuid()"))
    .withColumn("color", lit("yellow"))
    .withColumnRenamed("tpep_pickup_datetime", "pep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "pep_dropoff_datetime")
    .writeStream
    .queryName("ingest_yellow_autoloader")
    .format("delta") 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata/ingest_yellow_autoloader.checkpoint") 
    .trigger(processingTime='1 second') # 
    #.trigger(once=True) # to demo trigger once
    .outputMode("append")
    .table("tripdata_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from taxidemo.tripdata_bronze;

# COMMAND ----------


