# Databricks notebook source

# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Reader & Writer
# MAGIC ##### Objectives
# MAGIC 1. Read from CSV files
# MAGIC 1. Read from JSON files
# MAGIC 1. Write DataFrame to files
# MAGIC 1. Write DataFrame to tables
# MAGIC 1. Write DataFrame to a Delta table
# MAGIC 
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>: `csv`, `json`, `option`, `schema`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>: `mode`, `option`, `parquet`, `format`, `saveAsTable`
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">StructType</a>: `toDDL`
# MAGIC 
# MAGIC ##### Spark Types
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">Types</a>: `ArrayType`, `DoubleType`, `IntegerType`, `LongType`, `StringType`, `StructType`, `StructField`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrameReader
# MAGIC Interface used to load a DataFrame from external storage systems
# MAGIC 
# MAGIC ```
# MAGIC spark.read.parquet("path/to/files")
# MAGIC ```
# MAGIC 
# MAGIC DataFrameReader is accessible through the SparkSession attribute `read`. This class includes methods to load DataFrames from different external storage systems.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from CSV files
# MAGIC Read from CSV with the DataFrameReader's `csv` method and the following options:
# MAGIC 
# MAGIC Tab separator, use first line as header, infer schema

# COMMAND ----------

usersCsvPath = "/mnt/training/ecommerce/users/users-500k.csv"

usersDF = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(usersCsvPath)
          )

usersDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark's Python API also allows you to specify the DataFrameReader options as parameters to the `csv` method

# COMMAND ----------

usersDF = (spark
           .read
           .csv(usersCsvPath, sep="\t", header=True, inferSchema=True)
          )

usersDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Manually define the schema by creating a `StructType` with column names and data types

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("email", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Read from CSV using this user-defined schema instead of inferring the schema

# COMMAND ----------

usersDF = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(userDefinedSchema)
           .csv(usersCsvPath)
          )

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, define the schema using <a href="https://en.wikipedia.org/wiki/Data_definition_language" target="_blank">data definition language (DDL)</a> syntax.

# COMMAND ----------

DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

usersDF = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(DDLSchema)
           .csv(usersCsvPath)
          )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from JSON files
# MAGIC 
# MAGIC Read from JSON with DataFrameReader's `json` method and the infer schema option

# COMMAND ----------

eventsJsonPath = "/mnt/training/ecommerce/events/events-500k.json"

eventsDF = (spark
            .read
            .option("inferSchema", True)
            .json(eventsJsonPath)
           )

eventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Read data faster by creating a `StructType` with the schema names and data types

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

eventsDF = (spark
            .read
            .schema(userDefinedSchema)
            .json(eventsJsonPath)
           )

# COMMAND ----------

# MAGIC %md
# MAGIC You can use the `StructType` Scala method `toDDL` to have a DDL-formatted string created for you.
# MAGIC 
# MAGIC In a Python notebook, create a Scala cell to create the string to copy and paste.

# COMMAND ----------

DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

eventsDF = (spark
            .read
            .schema(DDLSchema)
            .json(eventsJsonPath)
           )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrameWriter
# MAGIC Interface used to write a DataFrame to external storage systems
# MAGIC 
# MAGIC ```
# MAGIC (df.write                         
# MAGIC   .option("compression", "snappy")
# MAGIC   .mode("overwrite")      
# MAGIC   .parquet(outPath)       
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC DataFrameWriter is accessible through the SparkSession attribute `write`. This class includes methods to write DataFrames to different external storage systems.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write DataFrames to files
# MAGIC 
# MAGIC Write `usersDF` to parquet with DataFrameWriter's `parquet` method and the following configurations:
# MAGIC 
# MAGIC Snappy compression, overwrite mode

# COMMAND ----------

usersOutputPath = workingDir + "/users.parquet"

(usersDF
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(usersOutputPath)
)

# COMMAND ----------

display(
    dbutils.fs.ls(usersOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC As with DataFrameReader, Spark's Python API also allows you to specify the DataFrameWriter options as parameters to the `parquet` method

# COMMAND ----------

(usersDF
 .write
 .parquet(usersOutputPath, compression="snappy", mode="overwrite")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write DataFrames to tables
# MAGIC 
# MAGIC Write `eventsDF` to a table using the DataFrameWriter method `saveAsTable`
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> This creates a global table, unlike the local view created by the DataFrame method `createOrReplaceTempView`

# COMMAND ----------

eventsDF.write.mode("overwrite").saveAsTable("events_p")

# COMMAND ----------

# MAGIC %md
# MAGIC This table was saved in the database created for you in classroom setup. See database name printed below.

# COMMAND ----------

print(databaseName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC 
# MAGIC In almost all cases, the best practice is to use Delta Lake format, especially whenever the data will be referenced from a Databricks workspace. 
# MAGIC 
# MAGIC <a href="https://delta.io/" target="_blank">Delta Lake</a> is an open source technology designed to work with Spark to bring reliability to data lakes.
# MAGIC 
# MAGIC ![delta](https://files.training.databricks.com/images/aspwd/delta_storage_layer.png)
# MAGIC 
# MAGIC #### Delta Lake's Key Features
# MAGIC - ACID transactions
# MAGIC - Scalable metadata handline
# MAGIC - Unified streaming and batch processing
# MAGIC - Time travel (data versioning)
# MAGIC - Schema enforcement and evolution
# MAGIC - Audit history
# MAGIC - Parquet format
# MAGIC - Compatible with Apache Spark API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Results to a Delta Table
# MAGIC 
# MAGIC Write `eventsDF` with the DataFrameWriter's `save` method and the following configurations: Delta format, overwrite mode

# COMMAND ----------

eventsOutputPath = workingDir + "/delta/events"

(eventsDF
 .write
 .format("delta")
 .mode("overwrite")
 .save(eventsOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingesting Data Lab
# MAGIC 
# MAGIC Read in CSV files containing products data.
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. Read with infer schema
# MAGIC 2. Read with user-defined schema
# MAGIC 3. Read with schema as DDL formatted string
# MAGIC 4. Write using Delta format

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method `fs.head` with the filepath provided in the variable `singleProductCsvFilePath`
# MAGIC - Create `productsDF` by reading from CSV files located in the filepath provided in the variable `productsCsvPath`
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

# ANSWER
singleProductCsvFilePath = "/mnt/training/ecommerce/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(singleProductCsvFilePath))

productsCsvPath = "/mnt/training/ecommerce/products/products.csv"

productsDF = (spark
              .read
              .option("header", True)
              .option("inferSchema", True)
              .csv(productsCsvPath)
             )

productsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

assert(productsDF.count() == 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read with user-defined schema
# MAGIC Define schema by creating a `StructType` with column names and data types

# COMMAND ----------

# ANSWER
userDefinedSchema = StructType([
    StructField("item_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True)
])

productsDF2 = (spark
               .read
               .option("header", True)
               .schema(userDefinedSchema)
               .csv(productsCsvPath)
              )

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

assert(userDefinedSchema.fieldNames() == ["item_id", "name", "price"])

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = productsDF2.first()

assert(expected1 == result1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read with DDL formatted string

# COMMAND ----------

# ANSWER
DDLSchema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

productsDF3 = (spark
               .read
               .option("header", True)
               .schema(DDLSchema)
               .csv(productsCsvPath)
              )

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

assert(productsDF3.count() == 12)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to Delta
# MAGIC Write `productsDF` to the filepath provided in the variable `productsOutputPath`

# COMMAND ----------

# ANSWER
productsOutputPath = workingDir + "/delta/products"
(productsDF
 .write
 .format("delta")
 .mode("overwrite")
 .save(productsOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(productsOutputPath)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == '_delta_log/':
        verify_delta_format = True
    elif f.name.endswith('.parquet'):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup
