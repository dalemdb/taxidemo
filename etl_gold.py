# Databricks notebook source
# MAGIC %md
# MAGIC #Taxi Demo
# MAGIC ### etl_gold notebook
# MAGIC <br />

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream TripData Silver into TripData Gold

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", cluster_cores)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Turn on Asynchronous checkpointing for lower latency (requires RocksDB state store config)

# COMMAND ----------

#spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "true")
#spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

tripdata_silver_df = (
  spark
    .readStream
    .option("maxFilesPerTrigger",cluster_cores)
    .table("tripdata_silver")
)
tripdata_silver_df.createOrReplaceTempView("tripdata_silver_stream")

# COMMAND ----------

from pyspark.sql.functions import col, sum, window

tripdata_etl_df = (
  tripdata_silver_df
    #.withWatermark("pickup_time", "1 day")  -- removed because our data is bounded and is not streamed in perfect sorted time order 
    .groupBy(
      "color", 
      "pickup_borough",
      "pickup_zone", 
      window("pickup_time","1 hour")
    )
    .agg(
      sum("passenger_count").alias("total_passengers"), 
      sum("total_amount").alias("total_amount"),
      sum("trip_distance").alias("total_distance"), 
      sum("trip_minutes").alias("total_minutes")
    )
    .select(
      col("window.start").alias("pickup_start_time"),
      col("window.end").alias("pickup_end_time"),
      "color", 
      "pickup_borough",
      "pickup_zone", 
      "total_passengers", 
      "total_amount",
      "total_distance", 
      "total_minutes"
    )
    
)

# COMMAND ----------

def processETL(batch_df, batch_id):
  batch_df.createOrReplaceTempView("tripdata_etl_batch")
  batch_df._jdf.sparkSession().sql("""
    MERGE INTO tripdata_gold tg
    USING tripdata_etl_batch tb
    ON  tg.color = tb.color AND
		tg.pickup_borough = tb.pickup_borough AND 
		tg.pickup_zone = tb.pickup_zone AND
        tg.pickup_start_time = tb.pickup_start_time AND
        tg.pickup_end_time = tb.pickup_start_time
    WHEN MATCHED
      THEN UPDATE SET *
    WHEN NOT MATCHED
      THEN INSERT *
  """)

(
   tripdata_etl_df
      .writeStream
      .queryName("etl_tripdata_gold")
      .option("checkpointLocation", f"abfss://lake@{lake_name}/gold/taxidemo/tripdata/etl_tripdata_gold.checkpoint") 
      .outputMode("update")
      .foreachBatch(processETL)
      .trigger(processingTime="1 second")
      .start()
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidemo.tripdata_gold
# MAGIC where total_passengers > 100
# MAGIC order by pickup_borough, pickup_zone, pickup_start_time asc
