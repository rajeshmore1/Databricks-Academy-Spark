# Databricks notebook source
# Does any work to reset the environment prior to testing.
import time
import re

try:
  dbutils.fs.unmount("/mnt/training")
except:
  print("/mnt/training isn't mounted")
  
time.sleep(15) # for some reason things are moving too fast

try:
  dbutils.fs.ls("/mnt/training")
  raise Exception("Mount shouldn't exist")
except:
  # This is good, mount has been removed
  print("/mnt/training confirmed to not exist")
  
# Drop any lingering databases
username = spark.sql("SELECT current_user()").first()[0].lower()
database_prefix = re.sub("[^a-zA-Z0-9]", "", username) + "_spark_programming_"

for database in spark.catalog.listDatabases():
  if database.name.startswith(database_prefix):
    print(f"Dropping {database.name}")
    spark.sql(f"DROP DATABASE IF EXISTS {database.name} CASCADE")

# COMMAND ----------

# MAGIC %run ./Classroom-Setup
