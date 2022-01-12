// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Datetime Functions
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Cast to timestamp
// MAGIC 2. Format datetimes
// MAGIC 3. Extract from timestamp
// MAGIC 4. Convert to date
// MAGIC 5. Manipulate datetimes
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.html#pyspark.sql.Column" target="_blank">Column</a>: `cast`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `date_format`, `to_date`, `date_add`, `year`, `month`, `dayofweek`, `minute`, `second`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use a subset of the BedBricks events dataset to practice working with date times.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Built-In Functions: Date Time Functions
// MAGIC Here are a few built-in functions to manipulate dates and times in Spark.
// MAGIC 
// MAGIC | Method | Description |
// MAGIC | --- | --- |
// MAGIC | add_months | Returns the date that is numMonths after startDate |
// MAGIC | current_timestamp | Returns the current timestamp at the start of query evaluation as a timestamp column |
// MAGIC | date_format | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
// MAGIC | dayofweek | Extracts the day of the month as an integer from a given date/timestamp/string |
// MAGIC | from_unixtime | Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format |
// MAGIC | minute | Extracts the minutes as an integer from a given date/timestamp/string. |
// MAGIC | unix_timestamp | Converts time string with given pattern to Unix timestamp (in seconds) |

// COMMAND ----------

// MAGIC %md
// MAGIC ### Cast to Timestamp

// COMMAND ----------

// MAGIC %md
// MAGIC #### `cast()`
// MAGIC Casts column to a different data type, specified using string representation or DataType.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Datetime Patterns for Formatting and Parsing
// MAGIC There are several common scenarios for datetime usage in Spark:
// MAGIC 
// MAGIC - CSV/JSON datasources use the pattern string for parsing and formatting datetime content.
// MAGIC - Datetime functions related to convert StringType to/from DateType or TimestampType e.g. `unix_timestamp`, `date_format`, `from_unixtime`, `to_date`, `to_timestamp`, etc.
// MAGIC 
// MAGIC Spark uses <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">pattern letters for date and timestamp parsing and formatting</a>. A subset of these patterns are shown below.
// MAGIC 
// MAGIC | Symbol | Meaning         | Presentation | Examples               |
// MAGIC | ------ | --------------- | ------------ | ---------------------- |
// MAGIC | G      | era             | text         | AD; Anno Domini        |
// MAGIC | y      | year            | year         | 2020; 20               |
// MAGIC | D      | day-of-year     | number(3)    | 189                    |
// MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
// MAGIC | d      | day-of-month    | number(3)    | 28                     |
// MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
// MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark's handling of dates and timestamps changed in version 3.0, and the patterns used for parsing and formatting these values changed as well. For a discussion of these changes, please reference <a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">this Databricks blog post</a>. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Format date

// COMMAND ----------

// MAGIC %md
// MAGIC #### `date_format()`
// MAGIC Converts a date/timestamp/string to a string formatted with the given date time pattern.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Extract datetime attribute from timestamp

// COMMAND ----------

// MAGIC %md
// MAGIC #### `year`
// MAGIC Extracts the year as an integer from a given date/timestamp/string.
// MAGIC 
// MAGIC ##### Similar methods: `month`, `dayofweek`, `minute`, `second`, etc.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Convert to Date

// COMMAND ----------

// MAGIC %md
// MAGIC #### `to_date`
// MAGIC Converts the column into DateType by casting rules to DateType.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Manipulate Datetimes

// COMMAND ----------

// MAGIC %md
// MAGIC #### `date_add`
// MAGIC Returns the date that is the given number of days after start

// COMMAND ----------

// MAGIC %md
// MAGIC # Active Users Lab
// MAGIC Plot daily active users and average active users by day of week.
// MAGIC 1. Extract timestamp and date of events
// MAGIC 2. Get daily active users
// MAGIC 3. Get average number of active users by day of week
// MAGIC 4. Sort day of week in correct order

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup
// MAGIC Run the cell below to create the starting DataFrame of user IDs and timestamps of events logged on the BedBricks website.

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Extract timestamp and date of events
// MAGIC - Convert **`ts`** from microseconds to seconds by dividing by 1 million and cast to timestamp
// MAGIC - Add **`date`** column by converting **`ts`** to date

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Get daily active users
// MAGIC - Group by date
// MAGIC - Aggregate approximate count of distinct **`user_id`** and alias to "active_users"
// MAGIC   - Recall built-in function to get approximate count distinct
// MAGIC - Sort by date
// MAGIC - Plot as line graph

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Get average number of active users by day of week
// MAGIC - Add **`day`** column by extracting day of week from **`date`** using a datetime pattern string
// MAGIC - Group by **`day`**
// MAGIC - Aggregate average of **`active_users`** and alias to "avg_users"

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
