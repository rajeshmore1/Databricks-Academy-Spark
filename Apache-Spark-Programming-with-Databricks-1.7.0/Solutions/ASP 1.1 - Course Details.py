# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache Spark Programming with Databricks
# MAGIC ##### Course Duration: 4 Half-Days
# MAGIC 
# MAGIC This course uses a case study driven approach to explore the fundamentals of Spark Programming with Databricks, including Spark architecture, the DataFrame API, query optimization, and Structured Streaming. You will start by defining Databricks and Spark, recognize their major components, and explore datasets for the case study using the Databricks environment. After ingesting data from various file formats, you will process and analyze datasets by applying a variety of DataFrame transformations, Column expressions, and built-in functions. Next, you will visualize and apply Spark architecture concepts in example scenarios. This will prepare you to explore the Spark UI and how caching, query optimization, and partitioning affect performance. Lastly, you will execute streaming queries to process and aggregate streaming data and learn about Delta Lake. 
# MAGIC 
# MAGIC ## Target Audience
# MAGIC This introductory-level course is suitable for anyone who wants to learn the fundamentals of programming in Spark, including SQL analysts, data engineers, data scientists, machine learning engineers, and data architects.
# MAGIC 
# MAGIC ## Requirements
# MAGIC - Familiarity with basic SQL concepts: select, filter, groupby, join, etc
# MAGIC - Beginner programming experience with Python: syntax, conditions, loops, functions
# MAGIC 
# MAGIC ## Course Objectives
# MAGIC Upon completion of this course, students should be able to meet the following objectives:
# MAGIC - Identify core features of Spark and Databricks
# MAGIC - Describe how DataFrames are created and evaluated in Spark
# MAGIC - Apply the DataFrame API to process and analyze data
# MAGIC - Demonstrate how Spark is optimized and executed on a cluster
# MAGIC - Apply Delta and Structured Streaming to process streaming data
# MAGIC 
# MAGIC ##### Related Certifications
# MAGIC - [Databricks Certified Associate Developer for Apache Spark 3.0](https://academy.databricks.com/exam/databricks-certified-associate-developer)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
