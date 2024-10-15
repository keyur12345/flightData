### Hourly Weather Summary

This solution generates an hourly weather summary for the given forecast period. For each hour within the forecast period, it
calculates and displays the following: </br>
• Average Wind Speed: Calculate the average wind speed for each hour. </br>
• Most Common Wind Direction: Determine the most frequent wind direction for each hour. </br>
• Cloud Coverage Summary: For each hour, provide a summary of cloud coverage, including the maximum cloud cover 
percentage and the average base height of clouds.

### Key Points
• Create a Catalog.schema.tablename - Catalog creation is not supported in this workspace so creating a Schema.Table instead </br>
• Skewed Partitions? Spark config is useful ("spark.sql.adaptive.coalescePartitions.enabled", "true"). Also, you can use repartition/coalesce/salting techniques. </br>
• You can increase the size limit for a small DF in the broadcast join. 10 MB is by default. </br>
• Autoloader for streaming pipeline. </br>
• Remove the display statements. </br>
• Unique ID should be named properly instead of "flightId". Few DF names can be upated to appropriate names based on the business context.
