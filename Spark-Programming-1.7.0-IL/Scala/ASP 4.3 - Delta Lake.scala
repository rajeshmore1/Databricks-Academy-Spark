// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Delta Lake
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Create a Delta Table
// MAGIC 1. Understand the transaction Log
// MAGIC 1. Read data from your Delta Table
// MAGIC 1. Update data in your Delta Table
// MAGIC 1. Access previous versions of table using time travel
// MAGIC 1. Vacuum
// MAGIC 
// MAGIC ##### Documentation
// MAGIC - <a href="https://docs.delta.io/latest/quick-start.html#create-a-table" target="_blank">Delta Table</a> 
// MAGIC - <a href="https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html" target="_blank">Transaction Log</a> 
// MAGIC - <a href="https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html" target="_blank">Time Travel</a> 

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create a Delta Table
// MAGIC Let's first read the Parquet-format BedBricks events dataset.

// COMMAND ----------

// MAGIC %md
// MAGIC Write the data in Delta format to the directory given by `deltaPath`.

// COMMAND ----------

// MAGIC %md
// MAGIC Write the data in Delta format as a managed table in the metastore.

// COMMAND ----------

// MAGIC %md
// MAGIC As with other file formats, Delta supports partitioning your data in storage using the unique values in a specified column (often referred to as "Hive partitioning").
// MAGIC 
// MAGIC Let's **overwrite** the Delta dataset in the `deltaPath` directory to partition by state. This can accelerate queries that filter by state.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Understand the Transaction Log
// MAGIC We can see how Delta stores the different state partitions in separate directories.
// MAGIC 
// MAGIC Additionally, we can also see a directory called `_delta_log`, which is the transaction log.
// MAGIC 
// MAGIC When a Delta Lake dataset is created, its transaction log is automatically created in the `_delta_log` subdirectory.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC When changes are made to that table, these changes are recorded as ordered, atomic commits in the transaction log.
// MAGIC 
// MAGIC Each commit is written out as a JSON file, starting with 00000000000000000000.json.
// MAGIC 
// MAGIC Additional changes to the table generate subsequent JSON files in ascending numerical order.
// MAGIC 
// MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://user-images.githubusercontent.com/20408077/87174138-609fe600-c29c-11ea-90cc-84df0c1357f1.png" width="500"/>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's take a look at a transaction log File.
// MAGIC 
// MAGIC 
// MAGIC The <a href="https://docs.databricks.com/delta/delta-utility.html" target="_blank">four columns</a> each represent a different part of the very first commit to the Delta Table, creating the table.
// MAGIC - The `add` column has statistics about the DataFrame as a whole and individual columns.
// MAGIC - The `commitInfo` column has useful information about what the operation was (WRITE or READ) and who executed the operation.
// MAGIC - The `metaData` column contains information about the column schema.
// MAGIC - The `protocol` version contains information about the minimum Delta version necessary to either write or read to this Delta Table.

// COMMAND ----------

// MAGIC %md
// MAGIC One key difference between these two transaction logs is the size of the JSON file, this file has 206 rows compared to the previous 7.
// MAGIC 
// MAGIC To understand why, let's take a look at the `commitInfo` column. We can see that in the `operationParameters` section, `partitionBy` has been filled in by the `state` column. Furthermore, if we look at the add section on row 3, we can see that a new section called `partitionValues` has appeared. As we saw above, Delta stores partitions separately in memory, however, it stores information about these partitions in the same transaction log file.

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's take a look at the files inside one of the state partitions. The files inside corresponds to the partition commit (file 01) in the _delta_log directory.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read from your Delta table

// COMMAND ----------

// MAGIC %md
// MAGIC ### Update your Delta Table
// MAGIC 
// MAGIC Let's filter for rows where the event takes place on a mobile device.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at the files in the California partition post-update. Remember, the different files in this directory are snapshots of your DataFrame corresponding to different commits.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Access previous versions of table using Time  Travel

// COMMAND ----------

// MAGIC %md
// MAGIC Oops, it turns out we actually we need the entire dataset! You can access a previous version of your Delta Table using Time Travel. Use the following two cells to access your version history. Delta Lake will keep a 30 day version history by default, but if necessary, Delta can store a version history for longer.

// COMMAND ----------

// MAGIC %md
// MAGIC Using the `versionAsOf` option allows you to easily access previous versions of our Delta Table.

// COMMAND ----------

// MAGIC %md
// MAGIC You can also access older versions using a timestamp.
// MAGIC 
// MAGIC Replace the timestamp string with the information from your version history. Note that you can use a date without the time information if necessary.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Vacuum
// MAGIC 
// MAGIC Now that we're happy with our Delta Table, we can clean up our directory using `VACUUM`. Vacuum accepts a retention period in hours as an input.

// COMMAND ----------

// MAGIC %md
// MAGIC It looks like our code doesn't run! By default, to prevent accidentally vacuuming recent commits, Delta Lake will not let users vacuum a period under 7 days or 168 hours. Once vacuumed, you cannot return to a prior commit through time travel, only your most recent Delta Table will be saved.

// COMMAND ----------

// MAGIC %md
// MAGIC We can workaround this by setting a spark configuration that will bypass the default retention period check.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at our Delta Table files now. After vacuuming, the directory only holds the partition of our most recent Delta Table commit.

// COMMAND ----------

// MAGIC %md
// MAGIC Since vacuuming deletes files referenced by the Delta Table, we can no longer access past versions. The code below should throw an error.

// COMMAND ----------

// MAGIC %md
// MAGIC # Delta Lab
// MAGIC ##### Tasks
// MAGIC 1. Write sales data to Delta
// MAGIC 1. Modify sales data to show item count instead of item array
// MAGIC 1. Rewrite sales data to same Delta path
// MAGIC 1. Create table and view version history
// MAGIC 1. Time travel to read previous version

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Write sales data to Delta
// MAGIC Write **`salesDF`** to **`deltaSalesPath`**

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Modify sales data to show item count instead of item array
// MAGIC Replace values in the **`items`** column with an integer value of the items array size.  
// MAGIC Assign the resulting DataFrame to **`updatedSalesDF`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Rewrite sales data to same Delta path
// MAGIC Write **`updatedSalesDF`** to the same Delta location **`deltaSalesPath`**.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> This will fail without an option to overwrite the schema.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Create table and view version history
// MAGIC Run SQL queries to perform the following steps.
// MAGIC - Drop table **`sales_delta`** if it exists
// MAGIC - Create **`sales_delta`** table using the **`deltaSalesPath`** location
// MAGIC - List version history for the **`sales_delta`** table

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Time travel to read previous version
// MAGIC Read delta table at **`deltaSalesPath`** at version 0.  
// MAGIC Assign the resulting DataFrame to **`oldSalesDF`**.

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
