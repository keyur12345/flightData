# Databricks notebook source
# MAGIC %md
# MAGIC ## Key Points
# MAGIC - Create a Catalog.schema.tablename - Catalog creation is not supported in this workspace so creating a Schema.Table instead
# MAGIC - Skewed Partitions? Spark config is useful ("spark.sql.adaptive.coalescePartitions.enabled", "true"). Also, you can use repartition/coalesce/salting technique
# MAGIC - You can increase the size limit for a small DF in the broadcast join. 10 MB is by default.
# MAGIC - Autoloader for streaming pipeline
# MAGIC - Remove the display()

# Import the libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import json
import time

# Saving the start time for calculating the total time taken
start = time.time()

# Initialize the Spark session and setting the relevant spark configs for the notebook
spark = SparkSession.builder \
        .appName("Hourly Flight Data") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true") \
        .getOrCreate()

# Create a list of dictionaries to simulate the data. Autoloader can be used for this kind of streaming application
flight_rdd = [
                {   
                    "stationId": "CYVR", 
                    "issueTime": "2022-03-30T20:40:00Z",
                    "forecastValidFrom": "2022-03-30T21:00:00Z", 
                    "forecastValidTo": "2022-03-31T09:00:00Z", 
                    "windDirection": "300", 
                    "windSpeed": "4.1", 
                    "cloudCoverage": '[{"cover":20,"baseHeight":600,"type":"NC"},{"cover":75,"baseHeight":1200,"type":"NC"}]',
                    "type": "INITIAL"
                },
                {   
                    "stationId": "CYVR", 
                    "issueTime": "2022-03-30T20:40:00Z",
                    "forecastValidFrom": "2022-03-30T21:00:00Z", 
                    "forecastValidTo": "2022-03-31T05:00:00Z", 
                    "windDirection": "300", 
                    "windSpeed": "4.1", 
                    "cloudCoverage": '[{"cover":45,"baseHeight":600,"type":"NC"},{"cover":90,"baseHeight":1800,"type":"NC"}]',
                    "type": "TEMPO"
                },
                {   
                    "stationId": "CYVR", 
                    "issueTime": "2022-03-30T20:40:00Z",
                    "forecastValidFrom": "2022-03-31T09:00:00Z", 
                    "forecastValidTo": "2022-03-31T18:00:00Z", 
                    "windDirection": "290", 
                    "windSpeed": "5.1", 
                    "cloudCoverage": '[{"cover":45,"baseHeight":450,"type":"NC"},{"cover":100,"baseHeight":900,"type":"NC"}]',
                    "type": "FM"
                },
                {   
                    "stationId": "CYVR", 
                    "issueTime": "2022-03-30T20:40:00Z",
                    "forecastValidFrom": "2022-03-31T18:00:00Z", 
                    "forecastValidTo": "2022-04-01T00:00:00Z", 
                    "windDirection": "270", 
                    "windSpeed": "5.1", 
                    "cloudCoverage": '[{"cover":20,"baseHeight":450,"type":"NC"},{"cover":85,"baseHeight":1500,"type":"NC"}]',
                    "type": "FM"
                },
                {   
                    "stationId": "CYVR", 
                    "issueTime": "2022-03-30T23:40:00Z",
                    "forecastValidFrom": "2022-03-31T00:00:00Z", 
                    "forecastValidTo": "2022-03-31T09:00:00Z", 
                    "windDirection": "270", 
                    "windSpeed": "4.1", 
                    "cloudCoverage": '[{"cover":20,"baseHeight":750,"type":"NC"},{"cover":90,"baseHeight":1600,"type":"NC"}]',
                    "type": "INITIAL"
                }
            ]

# Define the schema for the incoming data
flight_schema = StructType(
                            [
                                StructField("stationId", StringType()),
                                StructField("issueTime", StringType()),
                                StructField("forecastValidFrom", StringType()),
                                StructField("forecastValidTo", StringType()),
                                StructField("windDirection", StringType()),
                                StructField("windSpeed", StringType()),
                                StructField("cloudCoverage", StringType()),                                 
                                StructField("type", StringType()),                
                            ]
                             )

# create a spark DataFrame using rdd and schema
flight_df = spark.createDataFrame(flight_rdd, schema=flight_schema)

#display intial 5 records of flight_df
flight_df.head(5)

# Sort the dataframe based on the issueTime column for incremental processing
flight_df = flight_df.sort(flight_df.issueTime)

# assign a key to uniquely identify a single record
flightWithId_df = flight_df.withColumn("flightId", F.monotonically_increasing_id()).persist()
# flightWithId_df.display()

# convert the data types of all columns to the appropriate data types
flightWithId_df = flightWithId_df.withColumn("issueTime", F.col("issueTime").cast("timestamp"))\
                                .withColumn("forecastValidFrom", F.col("forecastValidFrom").cast("timestamp"))\
                                .withColumn("forecastValidTo", F.col("forecastValidTo").cast("timestamp"))\
                                .withColumn("windDirection", F.col("windDirection").cast("integer"))\
                                .withColumn("windSpeed", F.format_number(F.col("windSpeed").cast("float"), 1).cast("decimal(3,1)"))
                
# Generate hourly timestamps
# Sequence() with a 1 hour interval can be used to generate a list of hourly timestamps and explode() can be used to explode that sequence to generate hourly timestamps
hourly_df = flightWithId_df.withColumn(
    "hourly_timestamps",
    F.sequence(
        F.col("forecastValidFrom"),
        F.col("forecastValidTo"),
        F.expr("INTERVAL 1 HOUR")
    )
).select(
    F.explode("hourly_timestamps").alias("hourly_timestamps")\
    , F.col("flightId")
    , F.col("stationId")
    , F.col("issueTime")
    , F.col("forecastValidFrom")
    , F.col("forecastValidTo")
    , F.col("windDirection")
    , F.col("windSpeed")
    , F.col("cloudCoverage")
    , F.col("type")
).persist()

# Show the resulting DataFrame
# hourly_df.display()

# Unfortunately, Buckets are not supported with Delta lakes in Databricks.
# However, depending on the business use case, we can define our partitions instead. Ideally, we should use hourly_timestamps as a partition key. But again, that would create 24*30*12 = 8640 small paritions (with hardly 5-10 records based on the overlapping hours) just to process a year's worth of data. So, here I am going with a bigger partition volume by creating year and month, hence Extracting yyyy-MM from the issueTime column to use as a partition key
updated_hourly_df = hourly_df.withColumn("yearMonth", F.date_format(F.col("hourly_timestamps"), "yyyy-MM"))
# display(updated_hourly_df)

# Assign appropriate data type for the hourly_timestamps
updated_hourly_df = updated_hourly_df.withColumn("hourly_timestamps", F.date_format(F.to_timestamp("hourly_timestamps", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), "yyyy-MM-dd HH:mm:ss"))

# Persist the DataFrame for easy retrieval
updated_hourly_df.persist()
# display(updated_hourly_df)

# partition key can be decided based on the business use case and the data volume 
partitioned_df = updated_hourly_df.repartition("yearMonth").persist()

# display(partitioned_df)

# Create a temp view of the partitioned data
partitioned_df.createOrReplaceTempView("flight_withHourlyTS")

# Find the latest flightId to map with the latest issue Date since we are relying on the latest forecasted data and corresponding entries to assign it to the respective hourly_timestamp
hourlyTimestamps_df = spark.sql("""
        SELECT hourly_timestamps, 
            MAX(flightId) AS flightId 
        FROM flight_withHourlyTS 
        GROUP BY hourly_timestamps
""")
# display(hourlyTimestamps_df)

# Perform broadcast join on smaller dataframe hourlyTimestamps_df to optimize it for hourly data processing
flight_withHourlyTS_updated = partitioned_df.join(F.broadcast(hourlyTimestamps_df), on=["flightId", "hourly_timestamps"], how="inner")
# display(flight_withHourlyTS_updated)

# Flatten cloudCoverage JSON into separate columns
flight_withHourlyTS_updated_rdd = flight_withHourlyTS_updated.rdd.flatMap(lambda row: [(row.hourly_timestamps, coverage['cover'], coverage['baseHeight'], coverage['type'])\
                                               for coverage in json.loads(row.cloudCoverage)])

# Create DataFrame for cloud coverage and assign appropriate column names
cloud_coverage_df = spark.createDataFrame(flight_withHourlyTS_updated_rdd, ["hourly_timestamps", "cover", "baseHeight", "type"])
cloud_coverage_df = cloud_coverage_df.withColumnRenamed("cover", "cloudCoverage_cover") \
                                        .withColumnRenamed("baseHeight", "cloudCoverage_baseHeight") \
                                        .withColumnRenamed("type", "cloudCoverage_type")
# display(cloud_coverage_df)

# Combined the data using flight_withHourlyTS_updated and cloud_coverage_df
all_up_df = flight_withHourlyTS_updated.join(cloud_coverage_df, on='hourly_timestamps', how='left')
all_up_df = all_up_df.select(
                            flight_withHourlyTS_updated["flightId"], 
                            flight_withHourlyTS_updated["stationId"], 
                            flight_withHourlyTS_updated["issueTime"],
                            flight_withHourlyTS_updated["hourly_timestamps"], 
                            flight_withHourlyTS_updated["yearMonth"],
                            flight_withHourlyTS_updated["windDirection"], 
                            flight_withHourlyTS_updated["windSpeed"], 
                            cloud_coverage_df["cloudCoverage_cover"], 
                            cloud_coverage_df["cloudCoverage_baseHeight"], 
                            cloud_coverage_df["cloudCoverage_type"], 
                            flight_withHourlyTS_updated["type"]
                        ).persist()
# display(all_up_df)

# partition key can be decided based on the business use case and the data volume 
all_up_df.repartition("yearMonth")

# Calculate metrics using SQL
all_up_df.createOrReplaceTempView("all_up_df")

# Average wind speed and cloud coverage metrics
all_metrics_except_windDirection = spark.sql("""
        SELECT hourly_timestamps, 
            ROUND(AVG(windSpeed),1) AS Avg_wind_speed,
            MAX(cloudCoverage_cover) AS max_cloud_cover,
            AVG(cloudCoverage_baseHeight) AS avg_cloud_baseHeight
        FROM all_up_df
        GROUP BY hourly_timestamps
        ORDER BY hourly_timestamps
""")
# all_metrics_except_windDirection.display()

# the most common wind direction
mode_df = spark.sql("""
        WITH cte1 AS (
            SELECT hourly_timestamps, windDirection, COUNT(windDirection) as wind_dir_Number,
                    ROW_NUMBER() OVER(PARTITION BY hourly_timestamps ORDER BY COUNT(windDirection) DESC) as rw
            FROM all_up_df 
            GROUP BY hourly_timestamps, windDirection
        )
        SELECT hourly_timestamps, windDirection 
        FROM cte1
        WHERE rw = 1
""") 
# mode_df.display()

# Combine metrics into final DataFrame and alias the output columns properly
final_df = all_metrics_except_windDirection.join(mode_df, on='hourly_timestamps').select(
    F.col("hourly_timestamps").alias("Hour"),
    F.col("Avg_wind_speed").alias("Average wind speed (knots)"),
    F.col("windDirection").alias("Most common wind direction (degree)"),
    F.col("max_cloud_cover").alias("Maximum cloud coverage (%)"),
    F.col("avg_cloud_baseHeight").alias("Average cloud base height (feet)")
).persist()
# display(final_df)

# Create a new database called "flightDB"
# spark.sql("CREATE DATABASE IF NOT EXISTS flightDB")

# Use the newly created database
spark.sql("USE flightDB")

# spark.sql("""
#           CREATE TABLE IF NOT EXISTS flights (
#               Hour string,
#               Average_wind_speed decimal(3,1),
#               Most_common_wind_direction int,
#               Maximum_cloud_coverage float,
#               Average_cloud_base_height int       
#           ) USING DELTA
#           TBLPROPERTIES (
#             'delta.columnMapping.mode' = 'name'
#             )
#           """)

# MAGIC %sql
# MAGIC -- ALTER TABLE flights 
# MAGIC -- SET TBLPROPERTIES (
# MAGIC --   'delta.columnMapping.mode' = 'name',
# MAGIC --   'delta.minReaderVersion' = '2',
# MAGIC --   'delta.minWriterVersion' = '5'
# MAGIC -- );
# MAGIC
# MAGIC -- ALTER TABLE flights 
# MAGIC -- RENAME COLUMN Average_wind_speed TO `Average wind speed (knots)`;

# MAGIC %sql
# MAGIC -- ALTER TABLE flights
# MAGIC -- RENAME COLUMN Most_common_wind_direction TO `Most common wind direction (degrees)`;

# MAGIC %sql
# MAGIC -- ALTER TABLE flights
# MAGIC -- RENAME COLUMN `Most common wind direction (degrees)` TO `Most common wind direction (degree)`;

# MAGIC %sql
# MAGIC -- ALTER TABLE flights
# MAGIC -- RENAME COLUMN Maximum_cloud_coverage TO `Maximum cloud coverage (%)`

# MAGIC %sql
# MAGIC -- ALTER TABLE flights
# MAGIC -- RENAME COLUMN Average_cloud_base_height TO `Average cloud base height (feet)`

# Column name adjustments based on the required output format
final_df = final_df.withColumn("Average wind speed (knots)", final_df["Average wind speed (knots)"].cast("decimal(3,1)")) \
                    .withColumn("Most common wind direction (degree)", final_df["Most common wind direction (degree)"].cast("int")) \
                    .withColumn("Maximum cloud coverage (%)", final_df["Maximum cloud coverage (%)"].cast("float")) \
                    .withColumn("Average cloud base height (feet)", final_df["Average cloud base height (feet)"].cast("int"))

# Using mergedSchema to enable schema evolution on the final delta table
final_df.write.option("mergeSchema", "true").mode("append").saveAsTable("flights")

# Use OPTIMIZE to reduce the number of files in the delta table
spark.sql(f"OPTIMIZE flights")

# Releasing resources from the memory after the process is completed
flightWithId_df.unpersist()
hourly_df.unpersist()
updated_hourly_df.unpersist()
partitioned_df.unpersist()
all_up_df.unpersist()
final_df.unpersist()

# Calculate the total execution time
end = time.time()
execution_time = end - start
print(f"Execution Time: {execution_time} seconds")

# Stop the Spark session
spark.stop()
