# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ###setup_tables notebook
# MAGIC 
# MAGIC #### Clear and define all Delta Lake Tables and load Zones Bronze

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup Tripdata Bronze Delta Table

# COMMAND ----------

dbutils.fs.rm(f"abfss://lake@{lake_name}/bronze/taxidemo/tripdata", True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS tripdata_bronze

# COMMAND ----------

sql = f"""
CREATE TABLE IF NOT EXISTS tripdata_bronze
(
  id string,
  color string,
  VendorID integer,
  pep_pickup_datetime string,
  pep_dropoff_datetime string,
  store_and_fwd_flag string,
  RatecodeID integer,
  PULocationID integer,
  DOLocationID integer,
  passenger_count integer,
  trip_distance double,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  ehail_fee double,
  improvement_surcharge double,
  total_amount double,
  payment_type integer,
  trip_type integer,
  congestion_surcharge double
)
USING delta
LOCATION 'abfss://lake@{lake_name}/bronze/taxidemo/tripdata'
-- TBLPROPERTIES (delta.autoOptimize.optimizeWrite = false, delta.autoOptimize.autoCompact = false)
"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup Tripdata Silver Delta Table

# COMMAND ----------

dbutils.fs.rm(f"abfss://lake@{lake_name}/silver/taxidemo/tripdata", True)

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS tripdata_silver

# COMMAND ----------

sql = f"""
CREATE TABLE IF NOT EXISTS tripdata_silver
(
  id string,
  color string,
  pickup_date date,
  pickup_time timestamp,  
  dropoff_time timestamp, 
  pickup_zone_id integer,
  pickup_borough string, 
  pickup_zone string, 
  dropoff_zone_id integer,
  dropoff_borough string, 
  dropoff_zone string, 
  passenger_count integer,
  trip_distance double,
  trip_minutes double, 
  tip_amount double, 
  total_amount double 
)
USING delta
LOCATION 'abfss://lake@{lake_name}/silver/taxidemo/tripdata'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = false, delta.autoOptimize.autoCompact = false)
"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup Tripdata Gold Delta Table

# COMMAND ----------

dbutils.fs.rm(f"abfss://lake@{lake_name}/gold/taxidemo/tripdata", True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS tripdata_gold

# COMMAND ----------

sql = f"""
CREATE TABLE IF NOT EXISTS tripdata_gold
(
  color string,
  pickup_start_time timestamp,
  pickup_end_time timestamp,
  pickup_borough string, 
  pickup_zone string, 
  total_passengers long, 
  total_distance double,
  total_minutes double,
  total_amount double 
)
USING delta
LOCATION 'abfss://lake@{lake_name}/gold/taxidemo/tripdata'
TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true)
"""
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup Zones Bronze Delta Table from source

# COMMAND ----------

dbutils.fs.rm(f"abfss://lake@{lake_name}/bronze/taxidemo/zones", True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS zones_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS zones_csv
# MAGIC USING csv
# MAGIC OPTIONS (header=true)
# MAGIC LOCATION 'dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv';

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS zones_bronze
# MAGIC (
# MAGIC   LocationID integer, 
# MAGIC   Borough string, 
# MAGIC   Zone string, 
# MAGIC   service_zone string
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 'abfss://lake@fieldengdeveastus2adls.dfs.core.windows.net/bronze/taxidemo/zones'

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO zones_bronze
# MAGIC SELECT LocationID, Borough, Zone, service_zone 
# MAGIC FROM zones_csv;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM zones_bronze;
