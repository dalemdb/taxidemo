# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ##setup_yellow_autoloader notebook
# MAGIC <br />
# MAGIC - Creates small JSON files staged in DBFS from source Yellow Taxi cab trips
# MAGIC - Copy into drop Blob Storage folder

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clear the drop folder in Blob Storage that will be the autoloader source

# COMMAND ----------

dbutils.fs.rm(f"wasbs://ingest@{blob_name}/drop", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pull the first month of Yellow Taxi CSV source files and repartition them into 10000 JSON files in DBFS

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")

# COMMAND ----------

from pyspark.sql.functions import col

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
      .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[6]*.csv.gz")
      .orderBy(col("tpep_pickup_datetime").asc())
)

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

yellow_df.repartitionByRange(100000,"tpep_pickup_datetime").write.format("json").save(f"wasbs://ingest@{blob_name}/drop")
