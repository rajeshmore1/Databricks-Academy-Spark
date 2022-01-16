# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hourly Activity by Traffic Lab
# MAGIC Process streaming data to display the total active users by traffic source with a 1 hour window.
# MAGIC 1. Cast to timestamp and add watermark for 2 hours
# MAGIC 2. Aggregate active users by traffic source for 1 hour windows
# MAGIC 3. Execute query with `display` and plot results
# MAGIC 5. Use query name to stop streaming query

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC Run the cells below to generate hourly JSON files of event data for July 3, 2020.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

# Directory of hourly events logged from the BedBricks website on July 3, 2020
hourlyEventsPath = f"{datasetsDir}/events/events-2020-07-03.json"

df = (spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .json(hourlyEventsPath)
     )

# COMMAND ----------

# MAGIC %md ### 1. Cast to timestamp and add watermark for 2 hours
# MAGIC - Add a **`createdAt`** column by dividing **`event_timestamp`** by 1M and casting to timestamp
# MAGIC - Set a watermark of 2 hours on the **`createdAt`** column
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`eventsDF`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

eventsDF = (df
            .withColumn("createdAt", (col("event_timestamp") / 1e6).cast("timestamp"))
            .withWatermark("createdAt", "2 hours")
           )

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert "StructField(createdAt,TimestampType,true" in str(eventsDF.schema)

# COMMAND ----------

# MAGIC %md ### 2. Aggregate active users by traffic source for 1 hour windows
# MAGIC - Set the default shuffle partitions to the number of cores on your cluster (not required, but runs faster)
# MAGIC - Group by **`traffic_source`** with 1-hour tumbling windows based on the **`createdAt`** column
# MAGIC - Aggregate the approximate count of distinct users per **`traffic_source`** and hour, and alias the column to "active_users"
# MAGIC - Select **`traffic_source`**, **`active_users`**, and the **`hour`** extracted from **`window.start`** with an alias of "hour"
# MAGIC - Sort by **`hour`** in ascending order
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`trafficDF`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import approx_count_distinct, hour, window

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

trafficDF = (eventsDF
             .groupBy("traffic_source", window(col("createdAt"), "1 hour"))
             .agg(approx_count_distinct("user_id").alias("active_users"))
             .select(col("traffic_source"), col("active_users"), hour(col("window.start")).alias("hour"))
             .sort("hour")
            )

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert str(trafficDF.schema) == "StructType(List(StructField(traffic_source,StringType,true),StructField(active_users,LongType,false),StructField(hour,IntegerType,true)))"

# COMMAND ----------

# MAGIC %md ### 3. Execute query with display() and plot results
# MAGIC - Use `display` to start **`trafficDF`** as a streaming query and display the resulting memory sink
# MAGIC   - Assign "hourly_traffic" as the name of the query by seting the **`streamName`** parameter of `display`
# MAGIC - Plot the streaming query results as a bar graph
# MAGIC - Configure the following plot options:
# MAGIC   - Keys: **`hour`**
# MAGIC   - Series groupings: **`traffic_source`**
# MAGIC   - Values: **`active_users`**

# COMMAND ----------

# ANSWER
display(trafficDF, streamName="hourly_traffic")

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**
# MAGIC 
# MAGIC - The bar chart should plot `hour` on the x-axis and `active_users` on the y-axis
# MAGIC - Six bars should appear at every hour for all traffic sources
# MAGIC - The chart should stop at hour 23

# COMMAND ----------

# MAGIC %md ### 4. Manage streaming query
# MAGIC - Iterate over SparkSession's list of active streams to find one with name "hourly_traffic"
# MAGIC - Stop the streaming query

# COMMAND ----------

# ANSWER
untilStreamIsReady("hourly_traffic")

for s in spark.streams.active:
    if s.name == "hourly_traffic":
        s.stop()

# COMMAND ----------

# MAGIC %md %md **CHECK YOUR WORK**  
# MAGIC Print all active streams to check that "hourly_traffic" is no longer there

# COMMAND ----------

for s in spark.streams.active:
    print(s.name)

# COMMAND ----------

# MAGIC %md ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
