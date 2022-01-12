// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Streaming Query
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Build streaming DataFrames
// MAGIC 1. Display streaming query results
// MAGIC 1. Write streaming query results
// MAGIC 1. Monitor streaming query
// MAGIC 
// MAGIC ##### Classes
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ### Build streaming DataFrames
// MAGIC 
// MAGIC Obtain an initial streaming DataFrame from a Parquet-format file source.

// COMMAND ----------

// MAGIC %md
// MAGIC Apply some transformations, producing new streaming DataFrames.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write streaming query results
// MAGIC 
// MAGIC Take the final streaming DataFrame (our result table) and write it to a file sink in "append" mode.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Monitor streaming query
// MAGIC 
// MAGIC Use the streaming query "handle" to monitor and control it.

// COMMAND ----------

// MAGIC %md
// MAGIC # Coupon Sales Lab
// MAGIC Process and append streaming data on transactions using coupons.
// MAGIC 1. Read data stream
// MAGIC 2. Filter for transactions with coupons codes
// MAGIC 3. Write streaming query results to Parquet
// MAGIC 4. Monitor streaming query
// MAGIC 5. Stop streaming query
// MAGIC 
// MAGIC ##### Classes
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.html#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.html#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Read data stream
// MAGIC - Use the schema stored in **`schema`**
// MAGIC - Set to process 1 file per trigger
// MAGIC - Read from Parquet files in the source directory specified by **`salesPath`**
// MAGIC 
// MAGIC Assign the resulting DataFrame to **`df`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Filter for transactions with coupon codes
// MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
// MAGIC - Filter for records where **`items.coupon`** is not null
// MAGIC 
// MAGIC Assign the resulting DataFrame to **`couponSalesDF`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Write streaming query results to parquet
// MAGIC - Configure the streaming query to write Parquet format files in "append" mode
// MAGIC - Set the query name to "coupon_sales"
// MAGIC - Set a trigger interval of 1 second
// MAGIC - Set the checkpoint location to **`couponsCheckpointPath`**
// MAGIC - Set the output path to **`couponsOutputPath`**
// MAGIC 
// MAGIC Start the streaming query and assign the resulting handle to **`couponSalesQuery`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Monitor streaming query
// MAGIC - Get the ID of streaming query and store it in **`queryID`**
// MAGIC - Get the status of streaming query and store it in **`queryStatus`**

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Stop streaming query
// MAGIC - Stop the streaming query

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. Verify the records were written in Parquet format

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup
// MAGIC Run the cell below to clean up resources.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
