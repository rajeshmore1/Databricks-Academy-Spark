# Databricks notebook source

# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrames and Transformations Review
# MAGIC ## De-Duping Data Lab
# MAGIC 
# MAGIC In this exercise, we're doing ETL on a file we've received from a customer. That file contains data about people, including:
# MAGIC 
# MAGIC * first, middle and last names
# MAGIC * gender
# MAGIC * birth date
# MAGIC * Social Security number
# MAGIC * salary
# MAGIC 
# MAGIC But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:
# MAGIC 
# MAGIC * In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL").
# MAGIC * The Social Security numbers aren't consistent either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").
# MAGIC 
# MAGIC If all of the name fields match -- if you disregard character case -- then the birth dates and salaries are guaranteed to match as well,
# MAGIC and the Social Security Numbers *would* match if they were somehow put in the same format.
# MAGIC 
# MAGIC Your job is to remove the duplicate records. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
# MAGIC * Preserve the data format of the columns. For example, if you write the first name column in all lowercase, you haven't met this requirement.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The initial dataset contains 103,000 records.
# MAGIC The de-duplicated result has 100,000 records.
# MAGIC 
# MAGIC Next, write the results in **Delta** format as a **single data file** to the directory given by the variable *deltaDestDir*.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Remember the relationship between the number of partitions in a DataFrame and the number of files written.
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Built-In Functions</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC It's helpful to look at the file first, so you can check the format. `dbutils.fs.head()` (or just `%fs head`) is a big help here.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/dataframes/people-with-dups.txt

# COMMAND ----------

# TODO

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
destFile = workingDir + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

# Complete your work here...


# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(deltaDestDir)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == '_delta_log/':
        verify_delta_format = True
    elif f.name.endswith('.parquet'):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files == 1, "Expected 1 data file written"

verify_record_count = spark.read.format("delta").load(deltaDestDir).count()
assert verify_record_count == 100000, "Expected 100000 records in final result"

del verify_files, verify_delta_format, verify_num_data_files, verify_record_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up classroom
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Cleanup"
