# Databricks notebook source
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "{}")""".format(events_path))
spark.sql("""CREATE TABLE IF NOT EXISTS sales USING parquet OPTIONS (path "{}")""".format(sales_path))
spark.sql("""CREATE TABLE IF NOT EXISTS users USING parquet OPTIONS (path "{}")""".format(users_path))
spark.sql("""CREATE TABLE IF NOT EXISTS products USING parquet OPTIONS (path "{}")""".format(products_path))

displayHTML("")

