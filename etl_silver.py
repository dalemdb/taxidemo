# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ### etl_silver notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream TripData Bronze into TripData Silver

# COMMAND ----------

from pyspark.sql.functions import col,round 

tripdata_bronze_df = (
  spark
    .readStream
    .format("delta")
    .option("maxFilesPerTrigger", cluster_cores * 2)
    .option("startingVersion", 0)
    .table("tripdata_bronze")
)

zones_bronze_pu_df = (
  spark.table("zones_bronze")
    .select(
      col("LocationID").alias("pickup_zone_id"),
      col("Borough").alias("pickup_borough"), 
      col("Zone").alias("pickup_zone")
    )
)
zones_bronze_do_df = (
  spark.table("zones_bronze")
    .select(
      col("LocationID").alias("dropoff_zone_id"),
      col("Borough").alias("dropoff_borough"), 
      col("Zone").alias("dropoff_zone")
    )
)

# COMMAND ----------



tripdata_etl_df = (
  tripdata_bronze_df
    .join(zones_bronze_pu_df, tripdata_bronze_df["PULocationID"] == zones_bronze_pu_df["pickup_zone_id"])
    .join(zones_bronze_do_df, tripdata_bronze_df["DOLocationID"] == zones_bronze_do_df["dropoff_zone_id"])
    .select(
        "id",
        "color",
        col("pep_pickup_datetime").cast("date").alias("pickup_date"),
        col("pep_pickup_datetime").cast("timestamp").alias("pickup_time"),
        col("pep_dropoff_datetime").cast("timestamp").alias("dropoff_time"),
        "pickup_zone_id",
        "pickup_borough",
        "pickup_zone",
        "dropoff_zone_id",
        "dropoff_borough",
        "dropoff_zone",
        "passenger_count",
        "trip_distance",
        round(
          (col("pep_dropoff_datetime").cast("timestamp").cast("long") - 
           col("pep_pickup_datetime").cast("timestamp").cast("long")) / 60).alias("trip_minutes"),
        "tip_amount",
        "total_amount"
     )
    .where(
      (col("pickup_time") > '2015-12-31T23:59:59') &  
      (col("pickup_time") < '2021-01-01T00:00:00') 
    )
)

# COMMAND ----------

(
    tripdata_etl_df
      .writeStream
      .queryName("etl_tripdata_silver")
      .option("checkpointLocation", f"abfss://lake@{lake_name}/silver/taxidemo/tripdata/etl_tripdata_silver.checkpoint") 
      .trigger(processingTime='1 second')
      .outputMode("append")
      .table("tripdata_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from tripdata_silver
