# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Datetime Functions
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Cast to timestamp
# MAGIC 2. Format datetimes
# MAGIC 3. Extract from timestamp
# MAGIC 4. Convert to date
# MAGIC 5. Manipulate datetimes
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column" target="_blank">Column</a>: `cast`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `date_format`, `to_date`, `date_add`, `year`, `month`, `dayofweek`, `minute`, `second`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md Let's use a subset of the BedBricks events dataset to practice working with date times.

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.parquet(eventsPath).select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

# MAGIC %md ### Built-In Functions: Date Time Functions
# MAGIC Here are a few built-in functions to manipulate dates and times in Spark.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | add_months | Returns the date that is numMonths after startDate |
# MAGIC | current_timestamp | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | date_format | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | dayofweek | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC | from_unixtime | Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format |
# MAGIC | minute | Extracts the minutes as an integer from a given date/timestamp/string. |
# MAGIC | unix_timestamp | Converts time string with given pattern to Unix timestamp (in seconds) |

# COMMAND ----------

# MAGIC %md ### Cast to Timestamp

# COMMAND ----------

# MAGIC %md #### `cast()`
# MAGIC Casts column to a different data type, specified using string representation or DataType.

# COMMAND ----------

timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestampDF)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestampDF)

# COMMAND ----------

# MAGIC %md ### Datetime Patterns for Formatting and Parsing
# MAGIC There are several common scenarios for datetime usage in Spark:
# MAGIC 
# MAGIC - CSV/JSON datasources use the pattern string for parsing and formatting datetime content.
# MAGIC - Datetime functions related to convert StringType to/from DateType or TimestampType e.g. `unix_timestamp`, `date_format`, `from_unixtime`, `to_date`, `to_timestamp`, etc.
# MAGIC 
# MAGIC Spark uses <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">pattern letters for date and timestamp parsing and formatting</a>. A subset of these patterns are shown below.
# MAGIC 
# MAGIC | Symbol | Meaning         | Presentation | Examples               |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | G      | era             | text         | AD; Anno Domini        |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark's handling of dates and timestamps changed in version 3.0, and the patterns used for parsing and formatting these values changed as well. For a discussion of these changes, please reference <a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">this Databricks blog post</a>. 

# COMMAND ----------

# MAGIC %md ### Format date

# COMMAND ----------

# MAGIC %md #### `date_format()`
# MAGIC Converts a date/timestamp/string to a string formatted with the given date time pattern.

# COMMAND ----------

from pyspark.sql.functions import date_format

formattedDF = (timestampDF
               .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
               .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
              )
display(formattedDF)

# COMMAND ----------

# MAGIC %md ### Extract datetime attribute from timestamp

# COMMAND ----------

# MAGIC %md #### `year`
# MAGIC Extracts the year as an integer from a given date/timestamp/string.
# MAGIC 
# MAGIC ##### Similar methods: `month`, `dayofweek`, `minute`, `second`, etc.

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetimeDF = (timestampDF
              .withColumn("year", year(col("timestamp")))
              .withColumn("month", month(col("timestamp")))
              .withColumn("dayofweek", dayofweek(col("timestamp")))
              .withColumn("minute", minute(col("timestamp")))
              .withColumn("second", second(col("timestamp")))
             )
display(datetimeDF)

# COMMAND ----------

# MAGIC %md ### Convert to Date

# COMMAND ----------

# MAGIC %md #### `to_date`
# MAGIC Converts the column into DateType by casting rules to DateType.

# COMMAND ----------

from pyspark.sql.functions import to_date

dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
display(dateDF)

# COMMAND ----------

# MAGIC %md ### Manipulate Datetimes

# COMMAND ----------

# MAGIC %md #### `date_add`
# MAGIC Returns the date that is the given number of days after start

# COMMAND ----------

from pyspark.sql.functions import date_add

plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus2DF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Active Users Lab
# MAGIC Plot daily active users and average active users by day of week.
# MAGIC 1. Extract timestamp and date of events
# MAGIC 2. Get daily active users
# MAGIC 3. Get average number of active users by day of week
# MAGIC 4. Sort day of week in correct order

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame of user IDs and timestamps of events logged on the BedBricks website.

# COMMAND ----------

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

# ANSWER
datetimeDF = (df
              .withColumn("ts", (col("ts") / 1e6).cast("timestamp"))
              .withColumn("date", to_date("ts"))
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

# ANSWER
from pyspark.sql.functions import approx_count_distinct

activeUsersDF = (datetimeDF
                 .groupBy("date")
                 .agg(approx_count_distinct("user_id").alias("active_users"))
                 .sort("date")
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

# ANSWER
from pyspark.sql.functions import date_format, avg

activeDowDF = (activeUsersDF
               .withColumn("day", date_format(col("date"), "E"))
               .groupBy("day")
               .agg(avg(col("active_users")).alias("avg_users"))
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

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
