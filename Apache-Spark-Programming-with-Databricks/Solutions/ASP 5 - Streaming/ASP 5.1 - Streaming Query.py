# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Streaming Query
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Build streaming DataFrames
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC 1. Monitor streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### Build streaming DataFrames
# MAGIC 
# MAGIC Obtain an initial streaming DataFrame from a Parquet-format file source.

# COMMAND ----------

schema = "device STRING, ecommerce STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name STRING, event_previous_timestamp BIGINT, event_timestamp BIGINT, geo STRUCT<city: STRING, state: STRING>, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source STRING, user_first_touch_timestamp BIGINT, user_id STRING"

df = (spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(eventsPath)
     )
df.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC Apply some transformations, producing new streaming DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

emailTrafficDF = (df
                  .filter(col("traffic_source") == "email")
                  .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                  .select("user_id", "event_timestamp", "mobile")
                 )
emailTrafficDF.isStreaming

# COMMAND ----------

# MAGIC %md ### Write streaming query results
# MAGIC 
# MAGIC Take the final streaming DataFrame (our result table) and write it to a file sink in "append" mode.

# COMMAND ----------

checkpointPath = f"{workingDir}/email_traffic/checkpoint"
outputPath = f"{workingDir}/email_traffic/output"

devicesQuery = (emailTrafficDF
                .writeStream
                .outputMode("append")
                .format("parquet")
                .queryName("email_traffic")
                .trigger(processingTime="1 second")
                .option("checkpointLocation", checkpointPath)
                .start(outputPath)
               )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor streaming query
# MAGIC 
# MAGIC Use the streaming query "handle" to monitor and control it.

# COMMAND ----------

devicesQuery.id

# COMMAND ----------

devicesQuery.status

# COMMAND ----------

devicesQuery.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devicesQuery.stop()

# COMMAND ----------

devicesQuery.awaitTermination()

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
