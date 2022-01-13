# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # User-Defined Functions
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Define a function
# MAGIC 1. Create and apply a UDF
# MAGIC 1. Register the UDF to use in SQL
# MAGIC 1. Create and register a UDF with Python decorator syntax
# MAGIC 1. Create and apply a Pandas (vectorized) UDF
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html?#pyspark.sql.functions.udf" target="_blank">UDF Registration (`spark.udf`)</a>: `register`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `udf`
# MAGIC - <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python.html#use-udf-with-dataframes" target="_blank">Python UDF Decorator</a>: `@udf`
# MAGIC - <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python-pandas.html#pandas-user-defined-functions" target="_blank">Pandas UDF Decorator</a>: `@pandas_udf`

# COMMAND ----------

# MAGIC %md ### User-Defined Function (UDF)
# MAGIC A custom column transformation function
# MAGIC 
# MAGIC - Can’t be optimized by Catalyst Optimizer
# MAGIC - Function is serialized and sent to executors
# MAGIC - Row data is deserialized from Spark's native binary format to pass to the UDF, and the results are serialized back into Spark's native format
# MAGIC - For Python UDFs, additional interprocess communication overhead between the executor and a Python interpreter running on each worker node

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md For this demo, we're going to use the sales data.

# COMMAND ----------

salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# MAGIC %md ### Define a function
# MAGIC 
# MAGIC Define a function (on the driver) to get the first letter of a string from the `email` field.

# COMMAND ----------

def firstLetterFunction(email):
    return email[0]

firstLetterFunction("annagray@kaufman.com")

# COMMAND ----------

# MAGIC %md ### Create and apply UDF
# MAGIC Register the function as a UDF. This serializes the function and sends it to executors to be able to transform DataFrame records.

# COMMAND ----------

firstLetterUDF = udf(firstLetterFunction)

# COMMAND ----------

# MAGIC %md Apply the UDF on the `email` column.

# COMMAND ----------

from pyspark.sql.functions import col

display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %md ### Register UDF to use in SQL
# MAGIC Register the UDF using `spark.udf.register` to also make it available for use in the SQL namespace.

# COMMAND ----------

salesDF.createOrReplaceTempView("sales")

firstLetterUDF = spark.udf.register("sql_udf", firstLetterFunction)

# COMMAND ----------

# You can still apply the UDF from Python
display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can now also apply the UDF from SQL
# MAGIC SELECT sql_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# MAGIC %md ### Use Decorator Syntax (Python Only)
# MAGIC 
# MAGIC Alternatively, you can define and register a UDF using <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">Python decorator syntax</a>. The `@udf` decorator parameter is the Column datatype the function returns.
# MAGIC 
# MAGIC You will no longer be able to call the local Python function (i.e., `firstLetterUDF("annagray@kaufman.com")` will not work).
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This example also uses <a href="https://docs.python.org/3/library/typing.html" target="_blank">Python type hints</a>, which were introduced in Python 3.5. Type hints are not required for this example, but instead serve as "documentation" to help developers use the function correctly. They are used in this example to emphasize that the UDF processes one record at a time, taking a single `str` argument and returning a `str` value.

# COMMAND ----------

# Our input/output is a string
@udf("string")
def firstLetterUDF(email: str) -> str:
    return email[0]

# COMMAND ----------

# MAGIC %md And let's use our decorator UDF here.

# COMMAND ----------

from pyspark.sql.functions import col

salesDF = spark.read.parquet("/mnt/training/ecommerce/sales/sales.parquet")
display(salesDF.select(firstLetterUDF(col("email"))))

# COMMAND ----------

# MAGIC %md ### Pandas/Vectorized UDFs
# MAGIC 
# MAGIC As of Spark 2.3, there are Pandas UDFs available in Python to improve the efficiency of UDFs. Pandas UDFs utilize Apache Arrow to speed up computation.
# MAGIC 
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Blog post</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentation</a>
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC 
# MAGIC The user-defined functions are executed using: 
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>, an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes with near-zero (de)serialization cost
# MAGIC * Pandas inside the function, to work with Pandas instances and APIs
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> As of Spark 3.0, you should **always** define your Pandas UDF using Python type hints.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorizedUDF(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorizedUDF(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorizedUDF = pandas_udf(vectorizedUDF, "string")

# COMMAND ----------

display(salesDF.select(vectorizedUDF(col("email"))))

# COMMAND ----------

# MAGIC %md We can also register these Pandas UDFs to the SQL namespace.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorizedUDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort Day Lab
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. Define a UDF to label the day of week
# MAGIC 1. Apply the UDF to label and sort by day of week
# MAGIC 1. Plot active users by day of week as a bar graph

# COMMAND ----------

# MAGIC %md Start with a DataFrame of the average number of active users by day of week.
# MAGIC 
# MAGIC This was the resulting `df` in a previous lab.

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark
      .read
      .parquet(eventsPath)
      .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
      .withColumn("date", to_date("ts"))
      .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
      .withColumn("day", date_format(col("date"), "E"))
      .groupBy("day").agg(avg(col("active_users")).alias("avg_users"))
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define UDF to label day of week
# MAGIC 
# MAGIC Use the **`labelDayOfWeek`** function provided below to create the UDF **`labelDowUDF`**

# COMMAND ----------

def labelDayOfWeek(day: str) -> str:
    dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
           "Fri": "5", "Sat": "6", "Sun": "7"}
    return dow.get(day) + "-" + day

# COMMAND ----------

# ANSWER
labelDowUDF = spark.udf.register("labelDow", labelDayOfWeek)

# COMMAND ----------

# MAGIC %md ### 2. Apply UDF to label and sort by day of week
# MAGIC - Update the **`day`** column by applying the UDF and replacing this column
# MAGIC - Sort by **`day`**
# MAGIC - Plot as a bar graph

# COMMAND ----------

# ANSWER
finalDF = (df
           .withColumn("day", labelDowUDF(col("day")))
           .sort("day")
          )
display(finalDF)

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
