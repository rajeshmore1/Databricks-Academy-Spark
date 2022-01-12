// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Reader & Writer
// MAGIC ##### Objectives
// MAGIC 1. Read from CSV files
// MAGIC 1. Read from JSON files
// MAGIC 1. Write DataFrame to files
// MAGIC 1. Write DataFrame to tables
// MAGIC 1. Write DataFrame to a Delta table
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>: `csv`, `json`, `option`, `schema`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>: `mode`, `option`, `parquet`, `format`, `saveAsTable`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">StructType</a>: `toDDL`
// MAGIC 
// MAGIC ##### Spark Types
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">Types</a>: `ArrayType`, `DoubleType`, `IntegerType`, `LongType`, `StringType`, `StructType`, `StructField`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrameReader
// MAGIC Interface used to load a DataFrame from external storage systems
// MAGIC 
// MAGIC ```
// MAGIC spark.read.parquet("path/to/files")
// MAGIC ```
// MAGIC 
// MAGIC DataFrameReader is accessible through the SparkSession attribute `read`. This class includes methods to load DataFrames from different external storage systems.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read from CSV files
// MAGIC Read from CSV with the DataFrameReader's `csv` method and the following options:
// MAGIC 
// MAGIC Tab separator, use first line as header, infer schema

// COMMAND ----------

// MAGIC %md
// MAGIC Spark's Python API also allows you to specify the DataFrameReader options as parameters to the `csv` method

// COMMAND ----------

// MAGIC %md
// MAGIC Manually define the schema by creating a `StructType` with column names and data types

// COMMAND ----------

// MAGIC %md
// MAGIC Read from CSV using this user-defined schema instead of inferring the schema

// COMMAND ----------

// MAGIC %md
// MAGIC Alternatively, define the schema using <a href="https://en.wikipedia.org/wiki/Data_definition_language" target="_blank">data definition language (DDL)</a> syntax.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read from JSON files
// MAGIC 
// MAGIC Read from JSON with DataFrameReader's `json` method and the infer schema option

// COMMAND ----------

// MAGIC %md
// MAGIC Read data faster by creating a `StructType` with the schema names and data types

// COMMAND ----------

// MAGIC %md
// MAGIC You can use the `StructType` Scala method `toDDL` to have a DDL-formatted string created for you.
// MAGIC 
// MAGIC In a Python notebook, create a Scala cell to create the string to copy and paste.

// COMMAND ----------

spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrameWriter
// MAGIC Interface used to write a DataFrame to external storage systems
// MAGIC 
// MAGIC ```
// MAGIC (df.write                         
// MAGIC   .option("compression", "snappy")
// MAGIC   .mode("overwrite")      
// MAGIC   .parquet(outPath)       
// MAGIC )
// MAGIC ```
// MAGIC 
// MAGIC DataFrameWriter is accessible through the SparkSession attribute `write`. This class includes methods to write DataFrames to different external storage systems.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write DataFrames to files
// MAGIC 
// MAGIC Write `usersDF` to parquet with DataFrameWriter's `parquet` method and the following configurations:
// MAGIC 
// MAGIC Snappy compression, overwrite mode

// COMMAND ----------

// MAGIC %md
// MAGIC As with DataFrameReader, Spark's Python API also allows you to specify the DataFrameWriter options as parameters to the `parquet` method

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write DataFrames to tables
// MAGIC 
// MAGIC Write `eventsDF` to a table using the DataFrameWriter method `saveAsTable`
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method `createOrReplaceTempView`

// COMMAND ----------

// MAGIC %md
// MAGIC This table was saved in the database created for you in classroom setup. See database name printed below.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Delta Lake
// MAGIC 
// MAGIC In almost all cases, the best practice is to use Delta Lake format, especially whenever the data will be referenced from a Databricks workspace. 
// MAGIC 
// MAGIC <a href="https://delta.io/" target="_blank">Delta Lake</a> is an open source technology designed to work with Spark to bring reliability to data lakes.
// MAGIC 
// MAGIC ![delta](https://files.training.databricks.com/images/aspwd/delta_storage_layer.png)
// MAGIC 
// MAGIC #### Delta Lake's Key Features
// MAGIC - ACID transactions
// MAGIC - Scalable metadata handline
// MAGIC - Unified streaming and batch processing
// MAGIC - Time travel (data versioning)
// MAGIC - Schema enforcement and evolution
// MAGIC - Audit history
// MAGIC - Parquet format
// MAGIC - Compatible with Apache Spark API

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write Results to a Delta Table
// MAGIC 
// MAGIC Write `eventsDF` with the DataFrameWriter's `save` method and the following configurations: Delta format, overwrite mode

// COMMAND ----------

// MAGIC %md
// MAGIC # Ingesting Data Lab
// MAGIC 
// MAGIC Read in CSV files containing products data.
// MAGIC 
// MAGIC ##### Tasks
// MAGIC 1. Read with infer schema
// MAGIC 2. Read with user-defined schema
// MAGIC 3. Read with schema as DDL formatted string
// MAGIC 4. Write using Delta format

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Read with infer schema
// MAGIC - View the first CSV file using DBUtils method `fs.head` with the filepath provided in the variable `singleProductCsvFilePath`
// MAGIC - Create `productsDF` by reading from CSV files located in the filepath provided in the variable `productsCsvPath`
// MAGIC   - Configure options to use first line as header and infer schema

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Read with user-defined schema
// MAGIC Define schema by creating a `StructType` with column names and data types

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Read with DDL formatted string

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Write to Delta
// MAGIC Write `productsDF` to the filepath provided in the variable `productsOutputPath`

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
