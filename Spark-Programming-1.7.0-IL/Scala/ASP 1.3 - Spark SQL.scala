// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # Spark SQL
// MAGIC 
// MAGIC Demonstrate fundamental concepts in Spark SQL using the DataFrame API.
// MAGIC 
// MAGIC ##### Objectives
// MAGIC 1. Run a SQL query
// MAGIC 1. Create a DataFrame from a table
// MAGIC 1. Write the same query using DataFrame transformations
// MAGIC 1. Trigger computation with DataFrame actions
// MAGIC 1. Convert between DataFrames and SQL
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#spark-session-apis" target="_blank">SparkSession</a>: `sql`, `table`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>:
// MAGIC   - Transformations:  `select`, `where`, `orderBy`
// MAGIC   - Actions: `show`, `count`, `take`
// MAGIC   - Other methods: `printSchema`, `schema`, `createOrReplaceTempView`

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup-SQL

// COMMAND ----------

// MAGIC %md
// MAGIC ## Multiple Interfaces
// MAGIC Spark SQL is a module for structured data processing with multiple interfaces.  
// MAGIC 
// MAGIC We can interact with Spark SQL in two ways:
// MAGIC 1. Executing SQL queries
// MAGIC 1. Working with the DataFrame API.

// COMMAND ----------

// MAGIC %md
// MAGIC **Method 1: Executing SQL queries**  
// MAGIC 
// MAGIC This is how we interacted with Spark SQL in the previous lesson.

// COMMAND ----------

// MAGIC %md
// MAGIC **Method 2: Working with the DataFrame API**
// MAGIC 
// MAGIC We can also express Spark SQL queries using the DataFrame API.  
// MAGIC The following cell returns a DataFrame containing the same results as those retrieved above.

// COMMAND ----------

// MAGIC %md
// MAGIC We'll go over the syntax for the DataFrame API later in the lesson, but you can see this builder design pattern allows us to chain a sequence of operations very similar to those we find in SQL.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Query Execution
// MAGIC We can express the same query using any interface. The Spark SQL engine generates the same query plan used to optimize and execute on our Spark cluster.
// MAGIC 
// MAGIC ![query execution engine](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Resilient Distributed Datasets (RDDs) are the low-level representation of datasets processed by a Spark cluster. In early versions of Spark, you had to write <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html" target="_blank">code manipulating RDDs directly</a>. In modern versions of Spark you should instead use the higher-level DataFrame APIs, which Spark automatically compiles into low-level RDD operations.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Spark API Documentation
// MAGIC 
// MAGIC To learn how we work with DataFrames in Spark SQL, let's first look at the Spark API documentation.  
// MAGIC The main Spark [documentation](https://spark.apache.org/docs/latest/) page includes links to API docs and helpful guides for each version of Spark.  
// MAGIC 
// MAGIC The [Scala API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html) and [Python API](https://spark.apache.org/docs/latest/api/python/index.html) are most commonly used, and it's often helpful to reference the documentation for both languages.  
// MAGIC Scala docs tend to be more comprehensive, and Python docs tend to have more code examples.
// MAGIC 
// MAGIC #### Navigating Docs for the Spark SQL Module
// MAGIC Find the Spark SQL module by navigating to `org.apache.spark.sql` in the Scala API or `pyspark.sql` in the Python API.  
// MAGIC The first class we'll explore in this module is the `SparkSession` class. You can find this by entering "SparkSession" in the search bar.

// COMMAND ----------

// MAGIC %md
// MAGIC ## SparkSession
// MAGIC The `SparkSession` class is the single entry point to all functionality in Spark using the DataFrame API. 
// MAGIC 
// MAGIC In Databricks notebooks, the SparkSession is created for you, stored in a variable called `spark`.

// COMMAND ----------

// MAGIC %md
// MAGIC The example from the beginning of this lesson used the SparkSession method `table` to create a DataFrame from the `products` table. Let's save this in the variable `productsDF`.

// COMMAND ----------

// MAGIC %md
// MAGIC Below are several additional methods we can use to create DataFrames. All of these can be found in the <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">documentation</a> for `SparkSession`.
// MAGIC 
// MAGIC #### `SparkSession` Methods
// MAGIC | Method | Description |
// MAGIC | --- | --- |
// MAGIC | sql | Returns a DataFrame representing the result of the given query | 
// MAGIC | table | Returns the specified table as a DataFrame |
// MAGIC | read | Returns a DataFrameReader that can be used to read data in as a DataFrame |
// MAGIC | range | Create a DataFrame with a column containing elements in a range from start to end (exclusive) with step value and number of partitions |
// MAGIC | createDataFrame | Creates a DataFrame from a list of tuples, primarily used for testing |

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use a SparkSession method to run SQL.

// COMMAND ----------

// MAGIC %md
// MAGIC ## DataFrames
// MAGIC Recall that expressing our query using methods in the DataFrame API returns results in a DataFrame. Let's store this in the variable `budgetDF`.
// MAGIC 
// MAGIC A **DataFrame** is a distributed collection of data grouped into named columns.

// COMMAND ----------

// MAGIC %md
// MAGIC We can use `display()` to output the results of a dataframe.

// COMMAND ----------

// MAGIC %md
// MAGIC The **schema** defines the column names and types of a dataframe.
// MAGIC 
// MAGIC Access a dataframe's schema using the `schema` attribute.

// COMMAND ----------

// MAGIC %md
// MAGIC View a nicer output for this schema using the `printSchema()` method.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Transformations
// MAGIC When we created `budgetDF`, we used a series of DataFrame transformation methods e.g. `select`, `where`, `orderBy`. 
// MAGIC 
// MAGIC ```
// MAGIC productsDF
// MAGIC   .select("name", "price")
// MAGIC   .where("price < 200")
// MAGIC   .orderBy("price")
// MAGIC ```
// MAGIC Transformations operate on and return DataFrames, allowing us to chain transformation methods together to construct new DataFrames.  
// MAGIC However, these operations can't execute on their own, as transformation methods are **lazily evaluated**. 
// MAGIC 
// MAGIC Running the following cell does not trigger any computation.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Actions
// MAGIC Conversely, DataFrame actions are methods that **trigger computation**.  
// MAGIC Actions are needed to trigger the execution of any DataFrame transformations. 
// MAGIC 
// MAGIC The `show` action causes the following cell to execute transformations.

// COMMAND ----------

// MAGIC %md
// MAGIC Below are several examples of <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a> actions.
// MAGIC 
// MAGIC ### DataFrame Action Methods
// MAGIC | Method | Description |
// MAGIC | --- | --- |
// MAGIC | show | Displays the top n rows of DataFrame in a tabular form |
// MAGIC | count | Returns the number of rows in the DataFrame |
// MAGIC | describe,  summary | Computes basic statistics for numeric and string columns |
// MAGIC | first, head | Returns the the first row |
// MAGIC | collect | Returns an array that contains all rows in this DataFrame |
// MAGIC | take | Returns an array of the first n rows in the DataFrame |

// COMMAND ----------

// MAGIC %md
// MAGIC `count` returns the number of records in a DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC `collect` returns an array of all rows in a DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Convert between DataFrames and SQL

// COMMAND ----------

// MAGIC %md
// MAGIC `createOrReplaceTempView` creates a temporary view based on the DataFrame. The lifetime of the temporary view is tied to the SparkSession that was used to create the DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC # Spark SQL Lab
// MAGIC 
// MAGIC ##### Tasks
// MAGIC 1. Create a DataFrame from the `events` table
// MAGIC 1. Display the DataFrame and inspect its schema
// MAGIC 1. Apply transformations to filter and sort `macOS` events
// MAGIC 1. Count results and take the first 5 rows
// MAGIC 1. Create the same DataFrame using a SQL query
// MAGIC 
// MAGIC ##### Methods
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html?highlight=sparksession" target="_blank">SparkSession</a>: `sql`, `table`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> transformations: `select`, `where`, `orderBy`
// MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> actions: `select`, `count`, `take`
// MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> methods: `printSchema`, `schema`, `createOrReplaceTempView`

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Create a DataFrame from the `events` table
// MAGIC - Use SparkSession to create a DataFrame from the `events` table

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Display DataFrame and inspect schema
// MAGIC - Use methods above to inspect DataFrame contents and schema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Apply transformations to filter and sort `macOS` events
// MAGIC - Filter for rows where `device` is `macOS`
// MAGIC - Sort rows by `event_timestamp`
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Use single and double quotes in your filter SQL expression

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Count results and take first 5 rows
// MAGIC - Use DataFrame actions to count and take rows

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Create the same DataFrame using SQL query
// MAGIC - Use SparkSession to run a SQL query on the `events` table
// MAGIC - Use SQL commands to write the same filter and sort query used earlier

// COMMAND ----------

// MAGIC %md
// MAGIC **CHECK YOUR WORK**
// MAGIC - You should only see `macOS` values in the `device` column
// MAGIC - The fifth row should be an event with timestamp `1592539226602157`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Classroom Cleanup

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Cleanup
