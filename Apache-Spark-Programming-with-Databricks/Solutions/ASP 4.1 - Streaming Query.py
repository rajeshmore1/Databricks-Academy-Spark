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

# MAGIC %run ./Includes/Classroom-Setup

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

checkpointPath = userhome + "/email_traffic/checkpoint"
outputPath = userhome + "/email_traffic/output"

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

devicesQuery.awaitTermination(5)

# COMMAND ----------

devicesQuery.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # Coupon Sales Lab
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to Parquet
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
# MAGIC 
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %md ### 1. Read data stream
# MAGIC - Use the schema stored in **`schema`**
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Parquet files in the source directory specified by **`salesPath`**
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`df`**.

# COMMAND ----------

schema = "order_id BIGINT, email STRING, transaction_timestamp BIGINT, total_item_quantity BIGINT, purchase_revenue_in_usd DOUBLE, unique_items BIGINT, items ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>"

# COMMAND ----------

# ANSWER
df = (spark
      .readStream
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(salesPath)
     )

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert df.isStreaming
assert df.columns == ["order_id", "email", "transaction_timestamp", "total_item_quantity", "purchase_revenue_in_usd", "unique_items", "items"]

# COMMAND ----------

# MAGIC %md ### 2. Filter for transactions with coupon codes
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`couponSalesDF`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, explode

couponSalesDF = (df
                 .withColumn("items", explode(col("items")))
                 .filter(col("items.coupon").isNotNull())
                )

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

schemaStr = str(couponSalesDF.schema)
assert "StructField(items,StructType(List(StructField(coupon" in schemaStr, "items column was not exploded"

# COMMAND ----------

# MAGIC %md ### 3. Write streaming query results to parquet
# MAGIC - Configure the streaming query to write Parquet format files in "append" mode
# MAGIC - Set the query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set the checkpoint location to **`couponsCheckpointPath`**
# MAGIC - Set the output path to **`couponsOutputPath`**
# MAGIC 
# MAGIC Start the streaming query and assign the resulting handle to **`couponSalesQuery`**.

# COMMAND ----------

# ANSWER

couponsCheckpointPath = workingDir + "/coupon-sales/checkpoint"
couponsOutputPath = workingDir + "/coupon-sales/output"

couponSalesQuery = (couponSalesDF
                    .writeStream
                    .outputMode("append")
                    .format("parquet")
                    .queryName("coupon_sales")
                    .trigger(processingTime="1 second")
                    .option("checkpointLocation", couponsCheckpointPath)
                    .start(couponsOutputPath)
                   )

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

untilStreamIsReady("coupon_sales")
assert couponSalesQuery.isActive
assert len(dbutils.fs.ls(couponsOutputPath)) > 0
assert len(dbutils.fs.ls(couponsCheckpointPath)) > 0
assert "coupon_sales" in couponSalesQuery.lastProgress["name"]

# COMMAND ----------

# MAGIC %md ### 4. Monitor streaming query
# MAGIC - Get the ID of streaming query and store it in **`queryID`**
# MAGIC - Get the status of streaming query and store it in **`queryStatus`**

# COMMAND ----------

# ANSWER
queryID = couponSalesQuery.id
print(queryID)

# COMMAND ----------

# ANSWER
queryStatus = couponSalesQuery.status
print(queryStatus)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert type(queryID) == str
assert list(queryStatus.keys()) == ["message", "isDataAvailable", "isTriggerActive"]

# COMMAND ----------

# MAGIC %md ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

# ANSWER
couponSalesQuery.stop()

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert not couponSalesQuery.isActive

# COMMAND ----------

# MAGIC %md ### 6. Verify the records were written in Parquet format

# COMMAND ----------

# ANSWER
display(spark.read.parquet(couponsOutputPath))

# COMMAND ----------

# MAGIC %md ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
