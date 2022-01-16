# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

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

# MAGIC %run ../../Includes/Classroom-Setup

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

# MAGIC %md ### 1-A: Get emails of converted users from transactions
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

# MAGIC %md #### 1-B: Check Your Work
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

# MAGIC %md #### 2-B: Check Your Work
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

# MAGIC %md #### 3-B: Check Your Work
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

# MAGIC %md #### 4-B: Check Your Work
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

# MAGIC %md #### 5-B: Check Your Work
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

# MAGIC %md #### 6-B: Check Your Work
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

# MAGIC %md ### Clean up classroom

# COMMAND ----------

classroom_cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
