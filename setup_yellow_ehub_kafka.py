# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ##setup_yellow_ehub_kafka notebook
# MAGIC <br />
# MAGIC - Creates small JSON files staged in DBFS from source Yellow Taxi cab trips
# MAGIC - Copy into drop Blob Storage folder

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pull Yellow Taxi CSV source files 

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col, to_json, struct, desc, lit
from pyspark.sql.avro.functions import to_avro, from_avro

yellow_schema = """
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

yellow_df = (
    spark
      .read
      .format("csv")
      .schema(yellow_schema)
      .option("header",True)
      .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[6-9]*.csv.gz")
      .orderBy(col("tpep_pickup_datetime").asc())
)

# COMMAND ----------

yellow_df.count()

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

yellow_avro_df = yellow_df.select(
  to_avro(struct(col("tpep_pickup_datetime"))).alias("key"), 
  to_avro(struct(col("*"))).alias("value")
)

# COMMAND ----------

display(yellow_avro_df)

# COMMAND ----------

test_read_df = yellow_avro_df.select(from_avro(col("value"),yellow_avro_schema).alias("value"))
display(test_read_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Event Hubs via Kafka API

# COMMAND ----------

 (
   yellow_avro_df
    .write
    .format("kafka") 
    .option("topic", "yellow_taxi")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("kafka.sasl.mechanism", "PLAIN") 
    .option("kafka.security.protocol", "SASL_SSL") 
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) 
    .option("kafka.session.timeout.ms", "60000") 
    .option("kafka.request.timeout.ms", "30000") 
    .option("kafka.group.id", "$Default") 
    .option("kafka.batch.size", 12800) 
    .option("failOnDataLoss", "true") 
    .save()
)
