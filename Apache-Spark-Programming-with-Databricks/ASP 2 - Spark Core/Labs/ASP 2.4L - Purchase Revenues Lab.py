# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Purchase Revenues Lab
# MAGIC 
# MAGIC Prepare dataset of events with purchase revenue.
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. Extract purchase revenue for each event
# MAGIC 2. Filter events where revenue is not null
# MAGIC 3. Check what types of events have revenue
# MAGIC 4. Drop unneeded column
# MAGIC 
# MAGIC ##### Methods
# MAGIC - DataFrame: `select`, `drop`, `withColumn`, `filter`, `dropDuplicates`
# MAGIC - Column: `isNotNull`

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md ### 1. Extract purchase revenue for each event
# MAGIC Add new column **`revenue`** by extracting **`ecommerce.purchase_revenue_in_usd`**

# COMMAND ----------

# TODO
revenueDF = eventsDF.FILL_IN
display(revenueDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

expected1 = [5830.0, 5485.0, 5289.0, 5219.1, 5180.0, 5175.0, 5125.0, 5030.0, 4985.0, 4985.0]
result1 = [row.revenue for row in revenueDF.sort(col("revenue").desc_nulls_last()).limit(10).collect()]

assert(expected1 == result1)

# COMMAND ----------

# MAGIC %md ### 2. Filter events where revenue is not null
# MAGIC Filter for records where **`revenue`** is not **`null`**

# COMMAND ----------

# TODO
purchasesDF = revenueDF.FILL_IN
display(purchasesDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert purchasesDF.filter(col("revenue").isNull()).count() == 0, "Nulls in 'revenue' column"

# COMMAND ----------

# MAGIC %md ### 3. Check what types of events have revenue
# MAGIC Find unique **`event_name`** values in **`purchasesDF`** in one of two ways:
# MAGIC - Select "event_name" and get distinct records
# MAGIC - Drop duplicate records based on the "event_name" only
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> There's only one event associated with revenues

# COMMAND ----------

# TODO
distinctDF = purchasesDF.FILL_IN
display(distinctDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Drop unneeded column
# MAGIC Since there's only one event type, drop **`event_name`** from **`purchasesDF`**.

# COMMAND ----------

# TODO
finalDF = purchasesDF.FILL_IN
display(finalDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(finalDF.columns) == expected_columns)

# COMMAND ----------

# MAGIC %md ### 5. Chain all the steps above excluding step 3

# COMMAND ----------

# TODO
finalDF = (eventsDF
  .FILL_IN
)

display(finalDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert(finalDF.count() == 180678)

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(finalDF.columns) == expected_columns)

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
