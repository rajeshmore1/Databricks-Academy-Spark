# Databricks notebook source

# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional Functions
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Apply built-in functions to generate data for new columns
# MAGIC 1. Apply DataFrame NA functions to handle null values
# MAGIC 1. Join DataFrames
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a>: `fill`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>:
# MAGIC   - Aggregate: `collect_set`
# MAGIC   - Collection: `explode`
# MAGIC   - Non-aggregate and miscellaneous: `col`, `lit`

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a> is a DataFrame submodule with methods for handling null values. Obtain an instance of DataFrameNaFunctions by accessing the `na` attribute of a DataFrame.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | drop | Returns a new DataFrame omitting rows with any, all, or a specified number of null values, considering an optional subset of columns |
# MAGIC | fill | Replace null values with the specified value for an optional subset of columns |
# MAGIC | replace | Returns a new DataFrame replacing a value with another value, considering an optional subset of columns |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Non-aggregate and Miscellaneous Functions
# MAGIC Here are a few additional non-aggregate and miscellaneous built-in functions.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | col / column | Returns a Column based on the given column name. |
# MAGIC | lit | Creates a Column of literal value |
# MAGIC | isnull | Return true iff the column is null |
# MAGIC | rand | Generate a random column with independent and identically distributed (i.i.d.) samples uniformly distributed in [0.0, 1.0) |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining DataFrames
# MAGIC The DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">`join`</a> method joins two DataFrames based on a given join expression. Several different types of joins are supported. For example:
# MAGIC 
# MAGIC ```
# MAGIC # Inner join based on equal values of a shared column called 'name' (i.e., an equi join)
# MAGIC df1.join(df2, 'name')
# MAGIC 
# MAGIC # Inner join based on equal values of the shared columns called 'name' and 'age'
# MAGIC df1.join(df2, ['name', 'age'])
# MAGIC 
# MAGIC # Full outer join based on equal values of a shared column called 'name'
# MAGIC df1.join(df2, 'name', 'outer')
# MAGIC 
# MAGIC # Left outer join based on an explicit column expression
# MAGIC df1.join(df2, df1['customer_name'] == df2['account_name'], 'left_outer')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Abandoned Carts Lab
# MAGIC Get abandoned cart items for email without purchases.
# MAGIC 1. Get emails of converted users from transactions
# MAGIC 2. Join emails with user IDs
# MAGIC 3. Get cart item history for each user
# MAGIC 4. Join cart item history with emails
# MAGIC 5. Filter for emails with abandoned cart items
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `join`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>: `collect_set`, `explode`, `lit`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html" target="_blank">DataFrameNaFunctions</a>: `fill`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`salesDF`**, **`usersDF`**, and **`eventsDF`**.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# sale transactions at BedBricks
salesDF = spark.read.parquet(salesPath)
display(salesDF)

# COMMAND ----------

# user IDs and emails at BedBricks
usersDF = spark.read.parquet(usersPath)
display(usersDF)

# COMMAND ----------

# events logged on the BedBricks website
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-A: Get emails of converted users from transactions
# MAGIC - Select the **`email`** column in **`salesDF`** and remove duplicates
# MAGIC - Add a new column **`converted`** with the value **`True`** for all rows
# MAGIC 
# MAGIC Save the result as **`convertedUsersDF`**.

# COMMAND ----------

# TODO
from pyspark.sql.functions import *
convertedUsersDF = (salesDF.FILL_IN
)
display(convertedUsersDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expectedColumns = ["email", "converted"]

expectedCount = 210370

assert convertedUsersDF.columns == expectedColumns, "convertedUsersDF does not have the correct columns"

assert convertedUsersDF.count() == expectedCount, "convertedUsersDF does not have the correct number of rows"

assert convertedUsersDF.select(col("converted")).first()[0] == True, "converted column not correct"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-A: Join emails with user IDs
# MAGIC - Perform an outer join on **`convertedUsersDF`** and **`usersDF`** with the **`email`** field
# MAGIC - Filter for users where **`email`** is not null
# MAGIC - Fill null values in **`converted`** as **`False`**
# MAGIC 
# MAGIC Save the result as **`conversionsDF`**.

# COMMAND ----------

# TODO
conversionsDF = (usersDF.FILL_IN
)
display(conversionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expectedColumns = ["email", "user_id", "user_first_touch_timestamp", "converted"]

expectedCount = 782749

expectedFalseCount = 572379

assert conversionsDF.columns == expectedColumns, "Columns are not correct"

assert conversionsDF.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversionsDF.count() == expectedCount, "There is an incorrect number of rows"

assert conversionsDF.filter(col("converted") == False).count() == expectedFalseCount, "There is an incorrect number of false entries in converted column"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-A: Get cart item history for each user
# MAGIC - Explode the **`items`** field in **`eventsDF`** with the results replacing the existing **`items`** field
# MAGIC - Group by **`user_id`**
# MAGIC   - Collect a set of all **`items.item_id`** objects for each user and alias the column to "cart"
# MAGIC 
# MAGIC Save the result as **`cartsDF`**.

# COMMAND ----------

# TODO
cartsDF = (eventsDF.FILL_IN
)
display(cartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expectedColumns = ["user_id", "cart"]

expectedCount = 488403

assert cartsDF.columns == expectedColumns, "Incorrect columns"

assert cartsDF.count() == expectedCount, "Incorrect number of rows"

assert cartsDF.select(col("user_id")).drop_duplicates().count() == expectedCount, "Duplicate user_ids present"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4-A: Join cart item history with emails
# MAGIC - Perform a left join on **`conversionsDF`** and **`cartsDF`** on the **`user_id`** field
# MAGIC 
# MAGIC Save result as **`emailCartsDF`**.

# COMMAND ----------

# TODO
emailCartsDF = conversionsDF.FILL_IN
display(emailCartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expectedColumns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expectedCount = 782749

expectedCartNullCount = 397799

assert emailCartsDF.columns == expectedColumns, "Columns do not match"

assert emailCartsDF.count() == expectedCount, "Counts do not match"

assert emailCartsDF.filter(col("cart").isNull()).count() == expectedCartNullCount, "Cart null counts incorrect from join"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5-A: Filter for emails with abandoned cart items
# MAGIC - Filter **`emailCartsDF`** for users where **`converted`** is False
# MAGIC - Filter for users with non-null carts
# MAGIC 
# MAGIC Save result as **`abandonedItemsDF`**.

# COMMAND ----------

# TODO
abandonedCartsDF = (emailCartsDF.FILL_IN
)
display(abandonedCartsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

expectedColumns = ["user_id", "email", "user_first_touch_timestamp", "converted", "cart"]

expectedCount = 204272

assert abandonedCartsDF.columns == expectedColumns, "Columns do not match"

assert abandonedCartsDF.count() == expectedCount, "Counts do not match"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6-A: Bonus Activity
# MAGIC Plot number of abandoned cart items by product

# COMMAND ----------

# TODO
abandonedItemsDF = (abandonedCartsDF.FILL_IN
)
display(abandonedItemsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6-B: Check Your Work
# MAGIC 
# MAGIC Run the following cell to verify that your solution works:

# COMMAND ----------

abandonedItemsDF.count()

# COMMAND ----------

expectedColumns = ["items", "count"]

expectedCount = 12

assert abandonedItemsDF.count() == expectedCount, "Counts do not match"

assert abandonedItemsDF.columns == expectedColumns, "Columns do not match"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
