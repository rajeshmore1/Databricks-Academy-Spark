// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Query Optimization
// MAGIC 
// MAGIC We'll explore query plans and optimizations for several examples including logical optimizations and exanples with and without predicate pushdown.
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Logical optimizations
// MAGIC 1. Predicate pushdown
// MAGIC 1. No predicate pushdown
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>: `explain`

// COMMAND ----------

// MAGIC %md
// MAGIC ![query optimization](https://files.training.databricks.com/images/aspwd/query_optimization_catalyst.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ![query optimization aqe](https://files.training.databricks.com/images/aspwd/query_optimization_aqe.png)

// COMMAND ----------

// MAGIC %md
// MAGIC Let’s run our set up cell, and get our initial DataFrame stored in the variable `df`. Displaying this DataFrame shows us events data.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md
// MAGIC ### Logical Optimization
// MAGIC 
// MAGIC `explain(..)` prints the query plans, optionally formatted by a given explain mode. Compare the following logical plan & physical plan, noting how Catalyst handled the multiple `filter` transformations.

// COMMAND ----------

// MAGIC %md
// MAGIC Of course, we could have written the query originally using a single `filter` condition ourselves. Compare the previous and following query plans.

// COMMAND ----------

// MAGIC %md
// MAGIC Of course, we wouldn't write the following code intentionally, but in a long, complex query you might not notice the duplicate filter conditions. Let's see what Catalyst does with this query.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Caching
// MAGIC 
// MAGIC By default the data of a DataFrame is present on a Spark cluster only while it is being processed during a query -- it is not automatically persisted on the cluster afterwards. (Spark is a data processing engine, not a data storage system.) You can explicity request Spark to persist a DataFrame on the cluster by invoking its `cache` method.
// MAGIC 
// MAGIC If you do cache a DataFrame, you should always explictly evict it from cache by invoking `unpersist` when you no longer need it.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_best_32.png" alt="Best Practice"> Caching a DataFrame can be appropriate if you are certain that you will use the same DataFrame multiple times, as in:
// MAGIC 
// MAGIC - Exploratory data analysis
// MAGIC - Machine learning model training
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Aside from those use cases, you should **not** cache DataFrames because it is likely that you'll *degrade* the performance of your application.
// MAGIC 
// MAGIC - Caching consumes cluster resources that could otherwise be used for task execution
// MAGIC - Caching can prevent Spark from performing query optimizations, as shown in the next example

// COMMAND ----------

// MAGIC %md
// MAGIC ### Predicate Pushdown
// MAGIC 
// MAGIC Here is example reading from a JDBC source, where Catalyst determines that *predicate pushdown* can take place.

// COMMAND ----------

// Ensure that the driver class is loaded
Class.forName("org.postgresql.Driver")

// COMMAND ----------

// MAGIC %md
// MAGIC Note the lack of a **Filter** and the presence of a **PushedFilters** in the **Scan**. The filter operation is pushed to the database and only the matching records are sent to Spark. This can greatly reduce the amount of data that Spark needs to ingest.

// COMMAND ----------

// MAGIC %md
// MAGIC ### No Predicate Pushdown
// MAGIC 
// MAGIC In comparison, caching the data before filtering eliminates the possibility for the predicate push down.

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to the **Scan** (the JDBC read) we saw in the previous example, here we also see the **InMemoryTableScan** followed by a **Filter** in the explain plan.
// MAGIC 
// MAGIC This means Spark had to read ALL the data from the database and cache it, and then scan it in cache to find the records matching the filter condition.

// COMMAND ----------

// MAGIC %md
// MAGIC Remember to clean up after ourselves!

// COMMAND ----------

// MAGIC %md
// MAGIC ### Clean up classroom

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
