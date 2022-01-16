# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Active Users Lab
# MAGIC Plot daily active users and average active users by day of week.
# MAGIC 1. Extract timestamp and date of events
# MAGIC 2. Get daily active users
# MAGIC 3. Get average number of active users by day of week
# MAGIC 4. Sort day of week in correct order

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame of user IDs and timestamps of events logged on the BedBricks website.

# COMMAND ----------

from pyspark.sql.functions import col

df = (spark
      .read
      .parquet(eventsPath)
      .select("user_id", col("event_timestamp").alias("ts"))
     )

display(df)

# COMMAND ----------

# MAGIC %md ### 1. Extract timestamp and date of events
# MAGIC - Convert **`ts`** from microseconds to seconds by dividing by 1 million and cast to timestamp
# MAGIC - Add **`date`** column by converting **`ts`** to date

# COMMAND ----------

# TODO
datetimeDF = (df.FILL_IN
)
display(datetimeDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

expected1a = StructType([StructField("user_id", StringType(), True),
                         StructField("ts", TimestampType(), True),
                         StructField("date", DateType(), True)])

result1a = datetimeDF.schema

assert expected1a == result1a, "datetimeDF does not have the expected schema"

# COMMAND ----------

import datetime

expected1b = datetime.date(2020, 6, 19)
result1b = datetimeDF.sort("date").first().date

assert expected1b == result1b, "datetimeDF does not have the expected date values"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get daily active users
# MAGIC - Group by date
# MAGIC - Aggregate approximate count of distinct **`user_id`** and alias to "active_users"
# MAGIC   - Recall built-in function to get approximate count distinct
# MAGIC - Sort by date
# MAGIC - Plot as line graph

# COMMAND ----------

# TODO
activeUsersDF = (datetimeDF.FILL_IN
)
display(activeUsersDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import LongType

expected2a = StructType([StructField("date", DateType(), True),
                         StructField("active_users", LongType(), False)])

result2a = activeUsersDF.schema

assert expected2a == result2a, "activeUsersDF does not have the expected schema"

# COMMAND ----------

expected2b = [(datetime.date(2020, 6, 19), 251573), (datetime.date(2020, 6, 20), 357215), (datetime.date(2020, 6, 21), 305055), (datetime.date(2020, 6, 22), 239094), (datetime.date(2020, 6, 23), 243117)]

result2b = [(row.date, row.active_users) for row in activeUsersDF.take(5)]

assert expected2b == result2b, "activeUsersDF does not have the expected values"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Get average number of active users by day of week
# MAGIC - Add **`day`** column by extracting day of week from **`date`** using a datetime pattern string
# MAGIC - Group by **`day`**
# MAGIC - Aggregate average of **`active_users`** and alias to "avg_users"

# COMMAND ----------

# TODO
activeDowDF = (activeUsersDF.FILL_IN
)
display(activeDowDF)

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.types import DoubleType

expected3a = StructType([StructField("day", StringType(), True),
                         StructField("avg_users", DoubleType(), True)])

result3a = activeDowDF.schema

assert expected3a == result3a, "activeDowDF does not have the expected schema"

# COMMAND ----------

expected3b = [("Fri", 247180.66666666666), ("Mon", 238195.5), ("Sat", 278482.0), ("Sun", 282905.5), ("Thu", 264620.0), ("Tue", 260942.5), ("Wed", 227214.0)]

result3b = [(row.day, row.avg_users) for row in activeDowDF.sort("day").collect()]

assert expected3b == result3b, "activeDowDF does not have the expected values"

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
