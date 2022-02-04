# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ### etl_bronze_yellow_kafka notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Event Hubs Kafka API stream source

# COMMAND ----------

yellow_avro_schema = """
 {
   "type":"record",
   "name":"topLevelRecord",
   "fields":
     [
       {"name":"VendorID","type":["int","null"]},
       {"name":"tpep_pickup_datetime","type":["string","null"]},
       {"name":"tpep_dropoff_datetime","type":["string","null"]},
       {"name":"passenger_count","type":["int","null"]},
       {"name":"trip_distance","type":["double","null"]},
       {"name":"RatecodeID","type":["int","null"]},
       {"name":"store_and_fwd_flag","type":["string","null"]},
       {"name":"PULocationID","type":["int","null"]},
       {"name":"DOLocationID","type":["int","null"]},
       {"name":"payment_type","type":["int","null"]},
       {"name":"fare_amount","type":["double","null"]},
       {"name":"extra","type":["double","null"]},
       {"name":"mta_tax","type":["double","null"]},
       {"name":"tip_amount","type":["double","null"]},
       {"name":"tolls_amount","type":["double","null"]},
       {"name":"improvement_surcharge","type":["double","null"]},
       {"name":"total_amount","type":["double","null"]},
       {"name":"congestion_surcharge","type":["double","null"]}
     ]
}
"""

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.avro.functions import from_avro


yellow_df = (
  spark
    .readStream
    .format("kafka") 
    .option("subscribe", "yellow_taxi")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) 
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("failOnDataLoss", "false") 
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 960000)
    .load()
    .withColumn("value",from_avro(col("value"),yellow_avro_schema))
    .select("value.*")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## EventHubs stream into TripData Bronze Delta sink

# COMMAND ----------

(
   yellow_df
    .withColumn("id", expr("uuid()"))
    .withColumn("color",lit("yellow"))
    .withColumnRenamed("tpep_pickup_datetime","pep_pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime","pep_dropoff_datetime")
    .writeStream
    .queryName("ingest_yellow_ehub_kafka")
    .format("delta") 
    .option("checkpointLocation", f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata/ingest_yellow_ehub_kafka.checkpoint") 
    .trigger(processingTime='1 second') 
    .outputMode("append")
    .table("tripdata_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from tripdata_bronze
