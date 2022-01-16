# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

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

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### 1. Read with infer schema
# MAGIC - View the first CSV file using DBUtils method `fs.head` with the filepath provided in the variable `singleProductCsvFilePath`
# MAGIC - Create `productsDF` by reading from CSV files located in the filepath provided in the variable `productsCsvPath`
# MAGIC   - Configure options to use first line as header and infer schema

# COMMAND ----------

# TODO
singleProductCsvFilePath = f"{datasetsDir}/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(FILL_IN)
 
productsCsvPath = f"{datasetsDir}/products/products.csv"
productsDF = FILL_IN
 
productsDF.printSchema()

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert(productsDF.count() == 12)

# COMMAND ----------

# MAGIC %md ### 2. Read with user-defined schema
# MAGIC Define schema by creating a `StructType` with column names and data types

# COMMAND ----------

# TODO
userDefinedSchema = FILL_IN

productsDF2 = FILL_IN

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert(userDefinedSchema.fieldNames() == ["item_id", "name", "price"])

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = productsDF2.first()

assert(expected1 == result1)

# COMMAND ----------

# MAGIC %md ### 3. Read with DDL formatted string

# COMMAND ----------

# TODO
DDLSchema = FILL_IN

productsDF3 = FILL_IN

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

assert(productsDF3.count() == 12)

# COMMAND ----------

# MAGIC %md ### 4. Write to Delta
# MAGIC Write `productsDF` to the filepath provided in the variable `productsOutputPath`

# COMMAND ----------

# TODO
productsOutputPath = workingDir + "/delta/products"
productsDF.FILL_IN

# COMMAND ----------

# MAGIC %md **CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(productsOutputPath)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files

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
