# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Complex Types
# MAGIC 
# MAGIC Explore built-in functions for working with collections and strings.
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Apply collection functions to process arrays
# MAGIC 1. Union DataFrames together
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: **`unionByName`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>:
# MAGIC   - Aggregate: **`collect_set`**
# MAGIC   - Collection: **`array_contains`**, **`element_at`**, **`explode`**
# MAGIC   - String: **`split`**

# COMMAND ----------

# MAGIC %md ### String Functions
# MAGIC Here are some of the built-in functions available for manipulating strings.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | translate | Translate any character in the src by a character in replaceString |
# MAGIC | regexp_replace | Replace all substrings of the specified string value that match regexp with rep |
# MAGIC | regexp_extract | Extract a specific group matched by a Java regex, from the specified string column |
# MAGIC | ltrim | Removes the leading space characters from the specified string column |
# MAGIC | lower | Converts a string column to lowercase |
# MAGIC | split | Splits str around matches of the given pattern |

# COMMAND ----------

# MAGIC %md ### Collection Functions
# MAGIC 
# MAGIC Here are some of the built-in functions available for working with arrays.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | array_contains | Returns null if the array is null, true if the array contains value, and false otherwise. |
# MAGIC | element_at | Returns element of array at given index. Array elements are numbered starting with **1**. |
# MAGIC | explode | Creates a new row for each element in the given array or map column. |
# MAGIC | collect_set | Returns a set of objects with duplicate elements eliminated. |

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC 
# MAGIC Here are some of the built-in aggregate functions available for creating arrays, typically from GroupedData.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | collect_list | Returns an array consisting of all values within the group. |
# MAGIC | collect_set | Returns an array consisting of all unique values within the group. |

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Extract item details from purchases
# MAGIC 
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Select the **`email`** and **`item.item_name`** fields
# MAGIC - Split the words in **`item_name`** into an array and alias the column to "details"
# MAGIC 
# MAGIC Assign the resulting DataFrame to **`details_df`**.

# COMMAND ----------

df = spark.read.parquet(sales_path)
display(df)

# COMMAND ----------

from pyspark.sql.functions import *

details_df = (df
             .withColumn("items", explode("items"))
             .select("email", "items.item_name")
             .withColumn("details", split(col("item_name"), " "))
            )
display(details_df)

# COMMAND ----------

# MAGIC %md So you can see that our **`details`** column is now an array containing the quality, size, and object type. 

# COMMAND ----------

# MAGIC %md ### 2. Extract size and quality options from mattress purchases
# MAGIC 
# MAGIC - Filter **`detailsDF`** for records where **`details`** contains "Mattress"
# MAGIC - Add a **`size`** column by extracting the element at position 2
# MAGIC - Add a **`quality`** column by extracting the element at position 1
# MAGIC 
# MAGIC Save the result as **`mattress_df`**.

# COMMAND ----------

mattress_df = (details_df
              .filter(array_contains(col("details"), "Mattress"))
              .withColumn("size", element_at(col("details"), 2))
              .withColumn("quality", element_at(col("details"), 1))
             )
display(mattress_df)

# COMMAND ----------

# MAGIC %md Next we're going to do the same thing for pillow purchases.

# COMMAND ----------

# MAGIC %md ### 3. Extract size and quality options from pillow purchases
# MAGIC - Filter **`details_df`** for records where **`details`** contains "Pillow"
# MAGIC - Add a **`size`** column by extracting the element at position 1
# MAGIC - Add a **`quality`** column by extracting the element at position 2
# MAGIC 
# MAGIC Note the positions of **`size`** and **`quality`** are switched for mattresses and pillows.
# MAGIC 
# MAGIC Save result as **`pillow_df`**.

# COMMAND ----------

pillow_df = (details_df
            .filter(array_contains(col("details"), "Pillow"))
            .withColumn("size", element_at(col("details"), 1))
            .withColumn("quality", element_at(col("details"), 2))
           )
display(pillow_df)

# COMMAND ----------

# MAGIC %md ### 4. Combine data for mattress and pillows
# MAGIC 
# MAGIC - Perform a union on **`mattress_df`** and **`pillow_df`** by column names
# MAGIC - Drop the **`details`** column
# MAGIC 
# MAGIC Save the result as **`union_df`**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">**`union`**</a> method resolves columns by position, as in standard SQL. You should use it only if the two DataFrames have exactly the same schema, including the column order. In contrast, the DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">**`unionByName`**</a> method resolves columns by name.

# COMMAND ----------

union_df = mattress_df.unionByName(pillow_df).drop("details")
display(union_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. List all size and quality options bought by each user
# MAGIC 
# MAGIC - Group rows in **`union_df`** by **`email`**
# MAGIC   - Collect the set of all items in **`size`** for each user and alias the column to "size options"
# MAGIC   - Collect the set of all items in **`quality`** for each user and alias the column to "quality options"
# MAGIC 
# MAGIC Save the result as **`options_df`**.

# COMMAND ----------

options_df = (union_df
             .groupBy("email")
             .agg(collect_set("size").alias("size options"),
                  collect_set("quality").alias("quality options"))
            )
display(options_df)

# COMMAND ----------

# MAGIC %md ### Clean up classroom
# MAGIC 
# MAGIC And lastly, we'll clean up the classroom.

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
