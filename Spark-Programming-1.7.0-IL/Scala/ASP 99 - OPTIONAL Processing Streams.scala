// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Activity by Traffic Lab
// MAGIC Process streaming data to display total active users by traffic source.
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Read data stream
// MAGIC 2. Get active users by traffic source
// MAGIC 3. Execute query with display() and plot results
// MAGIC 4. Execute the same streaming query with DataStreamWriter
// MAGIC 5. View results being updated in the query table
// MAGIC 6. List and stop all active streams
// MAGIC 
// MAGIC ##### Classes
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=datastreamreader#pyspark.sql.streaming.DataStreamReader" target="_blank">DataStreamReader</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=datastreamwriter#pyspark.sql.streaming.DataStreamWriter" target="_blank">DataStreamWriter</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=streamingquery#pyspark.sql.streaming.StreamingQuery" target="_blank">StreamingQuery</a>
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=streamingquerymanager#pyspark.sql.streaming.StreamingQueryManager" target="_blank">StreamingQueryManager</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run the cells below to generate data and create the **`schema`** string needed for this lab.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Read data stream
// MAGIC - Use schema stored in **`schema`**
// MAGIC - Set to process 1 file per trigger
// MAGIC - Read from parquet with filepath stored in **`eventsPath`**
// MAGIC 
// MAGIC Assign the resulting DataFrame to **`df`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Get active users by traffic source
// MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
// MAGIC - Group by **`traffic_source`**
// MAGIC   - Aggregate the approximate count of distinct users and alias with "active_users"
// MAGIC - Sort by **`traffic_source`**

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Execute query with display() and plot results
// MAGIC - Execute results for **`trafficDF`** using display()
// MAGIC - Plot the streaming query results as a bar graph

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**
// MAGIC - You bar chart should plot `traffic_source` on the x-axis and `active_users` on the y-axis
// MAGIC - The top three traffic sources in descending order should be `google`, `facebook`, and `instagram`.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Execute the same streaming query with DataStreamWriter
// MAGIC - Name the query "active_users_by_traffic"
// MAGIC - Set to "memory" format and "complete" output mode
// MAGIC - Set a trigger interval of 1 second

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. View results being updated in the query table
// MAGIC Run a query in a SQL cell to display the results from the **`active_users_by_traffic`** table

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**
// MAGIC Your query should eventually result in the following values.
// MAGIC 
// MAGIC |traffic_source|active_users|
// MAGIC |---|---|
// MAGIC |direct|438886|
// MAGIC |email|281525|
// MAGIC |facebook|956769|
// MAGIC |google|1781961|
// MAGIC |instagram|530050|
// MAGIC |youtube|253321|

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. List and stop all active streams
// MAGIC - Use SparkSession to get list of all active streams
// MAGIC - Iterate over the list and stop each query

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup
// MAGIC Run the cell below to clean up resources.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
