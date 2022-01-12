# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Databricks Platform
# MAGIC 
# MAGIC Demonstrate basic functionality and identify terms related to working in the Databricks workspace.
# MAGIC 
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 1. Execute code in multiple languages
# MAGIC 1. Create documentation cells
# MAGIC 1. Access DBFS (Databricks File System)
# MAGIC 1. Create database and table
# MAGIC 1. Query table and plot results
# MAGIC 1. Add notebook parameters with widgets
# MAGIC 
# MAGIC 
# MAGIC ##### Databricks Notebook Utilities
# MAGIC - <a href="https://docs.databricks.com/notebooks/notebooks-use.html#language-magic" target="_blank">Magic commands</a>: `%python`, `%scala`, `%sql`, `%r`, `%sh`, `%md`
# MAGIC - <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a>: `dbutils.fs` (`%fs`), `dbutils.notebooks` (`%run`), `dbutils.widgets`
# MAGIC - <a href="https://docs.databricks.com/notebooks/visualizations/index.html" target="_blank">Visualization</a>: `display`, `displayHTML`

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC Run classroom setup to mount Databricks training datasets and create your own database for BedBricks.
# MAGIC 
# MAGIC Use the `%run` magic command to run another notebook within a notebook

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md ### Execute code in multiple languages
# MAGIC Run default language of notebook

# COMMAND ----------

print("Run default language")

# COMMAND ----------

# MAGIC %md Run language specified by language magic commands: `%python`, `%scala`, `%sql`, `%r`

# COMMAND ----------

# MAGIC %python
# MAGIC print("Run python")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Run scala")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %r
# MAGIC print("Run R", quote=FALSE)

# COMMAND ----------

# MAGIC %md Run shell commands on the driver using the magic command: `%sh`

# COMMAND ----------

# MAGIC %sh ps | grep 'java'

# COMMAND ----------

# MAGIC %md Render HTML using the function: `displayHTML` (available in Python, Scala, and R)

# COMMAND ----------

html = """<h1 style="color:orange;text-align:center;font-family:Courier">Render HTML</h1>"""
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create documentation cells
# MAGIC Render cell as <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">Markdown</a> using the magic command: `%md`  
# MAGIC 
# MAGIC Below are some examples of how you can use Markdown to format documentation. Click this cell and press `Enter` to view the underlying Markdown syntax.
# MAGIC 
# MAGIC 
# MAGIC # Heading 1
# MAGIC ### Heading 3
# MAGIC > block quote
# MAGIC 
# MAGIC 1. **bold**
# MAGIC 2. *italicized*
# MAGIC 3. ~~strikethrough~~
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC - [link](https://www.markdownguide.org/cheat-sheet/)
# MAGIC - `code`
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC   "message": "This is a code block",
# MAGIC   "method": "https://www.markdownguide.org/extended-syntax/#fenced-code-blocks",
# MAGIC   "alternative": "https://www.markdownguide.org/basic-syntax/#code-blocks"
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ![Spark Logo](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC 
# MAGIC | Element         | Markdown Syntax |
# MAGIC |-----------------|-----------------|
# MAGIC | Heading         | `#H1` `##H2` `###H3` `#### H4` `##### H5` `###### H6` |
# MAGIC | Block quote     | `> blockquote` |
# MAGIC | Bold            | `**bold**` |
# MAGIC | Italic          | `*italicized*` |
# MAGIC | Strikethrough   | `~~strikethrough~~` |
# MAGIC | Horizontal Rule | `---` |
# MAGIC | Code            | ``` `code` ``` |
# MAGIC | Link            | `[text](https://www.example.com)` |
# MAGIC | Image           | `[alt text](image.jpg)`|
# MAGIC | Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
# MAGIC | Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
# MAGIC | Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
# MAGIC | Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|

# COMMAND ----------

# MAGIC %md ## Access DBFS (Databricks File System)
# MAGIC The <a href="https://docs.databricks.com/data/databricks-file-system.html" target="_blank">Databricks File System</a> (DBFS) is a virtual file system that allows you to treat cloud object storage as though it were local files and directories on the cluster.
# MAGIC 
# MAGIC Run file system commands on DBFS using the magic command: `%fs`

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/README.md

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %md `%fs` is shorthand for the <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">DBUtils</a> module: `dbutils.fs`

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

# MAGIC %md 
# MAGIC Run file system commands on DBFS using DBUtils directly

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md Visualize results in a table using the Databricks <a href="https://docs.databricks.com/notebooks/visualizations/index.html#display-function-1" target="_blank">display</a> function

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets")
display(files)

# COMMAND ----------

# MAGIC %md ## Create table
# MAGIC Run <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/index.html#sql-reference" target="_blank">Databricks SQL Commands</a> to create a table named `events` using BedBricks event files on DBFS.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

# COMMAND ----------

# MAGIC %md This table was saved in the database created for you in the classroom setup. See the database name printed below.

# COMMAND ----------

print(databaseName)

# COMMAND ----------

# MAGIC %md View your database and table in the Data tab of the UI.

# COMMAND ----------

# MAGIC %md ## Query table and plot results
# MAGIC Use SQL to query the `events` table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events

# COMMAND ----------

# MAGIC %md Run the query below and then <a href="https://docs.databricks.com/notebooks/visualizations/index.html#plot-types" target="_blank">plot</a> results by selecting the bar chart icon.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT traffic_source, SUM(ecommerce.purchase_revenue_in_usd) AS total_revenue
# MAGIC FROM events
# MAGIC GROUP BY traffic_source

# COMMAND ----------

# MAGIC %md ## Add notebook parameters with widgets
# MAGIC Use <a href="https://docs.databricks.com/notebooks/widgets.html" target="_blank">widgets</a> to add input parameters to your notebook.
# MAGIC 
# MAGIC Create a text input widget using SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT state DEFAULT "CA"

# COMMAND ----------

# MAGIC %md Access the current value of the widget using the function `getArgument`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM events
# MAGIC WHERE geo.state = getArgument("state")

# COMMAND ----------

# MAGIC %md Remove the text widget

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET state

# COMMAND ----------

# MAGIC %md To create widgets in Python, Scala, and R, use the DBUtils module: `dbutils.widgets`

# COMMAND ----------

dbutils.widgets.text("name", "Brickster", "Name")
dbutils.widgets.multiselect("colors", "orange", ["red", "orange", "black", "blue"], "Traffic Sources")

# COMMAND ----------

# MAGIC %md Access the current value of the widget using the `dbutils.widgets` function `get`

# COMMAND ----------

name = dbutils.widgets.get("name")
colors = dbutils.widgets.get("colors").split(",")

html = "<div>Hi {}! Select your color preference.</div>".format(name)
for c in colors:
    html += """<label for="{}" style="color:{}"><input type="radio"> {}</label><br>""".format(c, c, c)

displayHTML(html)

# COMMAND ----------

# MAGIC %md Remove all widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore Datasets Lab
# MAGIC 
# MAGIC We will use tools introduced in this lesson to explore the datasets used in this course.
# MAGIC 
# MAGIC ### BedBricks Case Study
# MAGIC This course uses a case study that explores clickstream data for the online mattress retailer, BedBricks.  
# MAGIC You are an analyst at BedBricks working with the following datasets: `events`, `sales`, `users`, and `products`.
# MAGIC 
# MAGIC ##### Tasks
# MAGIC 1. View data files in DBFS using magic commands
# MAGIC 1. View data files in DBFS using dbutils
# MAGIC 1. Create tables from files in DBFS
# MAGIC 1. Execute SQL to answer questions on BedBricks datasets

# COMMAND ----------

# MAGIC %md ### 1. List data files in DBFS using magic commands
# MAGIC Use a magic command to display files located in the DBFS directory: **`/mnt/training/ecommerce`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see four items: `events`, `products`, `sales`, `users`

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md ### 2. List data files in DBFS using dbutils
# MAGIC - Use **`dbutils`** to get the files at the directory above and save it to the variable **`files`**
# MAGIC - Use the Databricks display() function to display the contents in **`files`**
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see four items: `events`, `items`, `sales`, `users`

# COMMAND ----------

# TODO
files = dbutils.FILL_IN
display(files)

# COMMAND ----------

# MAGIC %md ### 3. Create tables below from files in DBFS
# MAGIC - Create `users` table using files at location `"/mnt/training/ecommerce/users/users.parquet"` 
# MAGIC - Create `sales` table using files at location `"/mnt/training/ecommerce/sales/sales.parquet"` 
# MAGIC - Create `products` table using files at location `"/mnt/training/ecommerce/products/products.parquet"` 
# MAGIC 
# MAGIC (We created `events` table earlier using files at location `"/mnt/training/ecommerce/events/events.parquet"`)

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md Use the data tab of the workspace UI to confirm your tables were created.

# COMMAND ----------

# MAGIC %md ### 4. Execute SQL to explore BedBricks datasets
# MAGIC Run SQL queries on the `products`, `sales`, and `events` tables to answer the following questions. 
# MAGIC - What products are available for purchase at BedBricks?
# MAGIC - What is the average purchase revenue for a transaction at BedBricks?
# MAGIC - What types of events are recorded on the BedBricks website?
# MAGIC 
# MAGIC The schema of the relevant dataset is provided for each question in the cells below.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Q1: What products are available for purchase at BedBricks?
# MAGIC 
# MAGIC The **`products`** dataset contains the ID, name, and price of products on the BedBricks retail site.
# MAGIC 
# MAGIC | field | type | description
# MAGIC | --- | --- | --- |
# MAGIC | item_id | string | unique item identifier |
# MAGIC | name | string | item name in plain text |
# MAGIC | price | double | price of item |
# MAGIC 
# MAGIC Execute a SQL query that selects all from the **`products`** table. 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 12 products.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md #### Q2: What is the average purchase revenue for a transaction at BedBricks?
# MAGIC 
# MAGIC The **`sales`** dataset contains order information representing successfully processed sales.  
# MAGIC Most fields correspond directly with fields from the clickstream data associated with a sale finalization event.
# MAGIC 
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | order_id | long | unique identifier |
# MAGIC | email | string | the email address to which sales configuration was sent |
# MAGIC | transaction_timestamp | long | timestamp at which the order was processed, recorded in milliseconds since epoch |
# MAGIC | total_item_quantity | long | number of individual items in the order |
# MAGIC | purchase_revenue_in_usd | double | total revenue from order |
# MAGIC | unique_items | long | number of unique products in the order |
# MAGIC | items | array | provided as a list of JSON data, which is interpreted by Spark as an array of structs |
# MAGIC 
# MAGIC Execute a SQL query that computes the average **`purchase_revenue_in_usd`** from the **`sales`** table.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The result should be `1042.79`.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md #### Q3: What types of events are recorded on the BedBricks website?
# MAGIC 
# MAGIC The **`events`** dataset contains two weeks worth of parsed JSON records, created by consuming updates to an operational database.  
# MAGIC Records are received whenever: (1) a new user visits the site, (2) a user provides their email for the first time.
# MAGIC 
# MAGIC | field | type | description|
# MAGIC | --- | --- | --- |
# MAGIC | device | string | operating system of the user device |
# MAGIC | user_id | string | unique identifier for user/session |
# MAGIC | user_first_touch_timestamp | long | first time the user was seen in microseconds since epoch |
# MAGIC | traffic_source | string | referral source |
# MAGIC | geo (city, state) | struct | city and state information derived from IP address |
# MAGIC | event_timestamp | long | event time recorded as microseconds since epoch |
# MAGIC | event_previous_timestamp | long | time of previous event in microseconds since epoch |
# MAGIC | event_name | string | name of events as registered in clickstream tracker |
# MAGIC | items (item_id, item_name, price_in_usd, quantity, item_revenue in usd, coupon)| array | an array of structs for each unique item in the user’s cart |
# MAGIC | ecommerce (total_item_quantity, unique_items, purchase_revenue_in_usd)  |  struct  | purchase data (this field is only non-null in those events that correspond to a sales finalization) |
# MAGIC 
# MAGIC Execute a SQL query that selects distinct values in **`event_name`** from the **`events`** table
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see 23 distinct **`event_name`** values.

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md ### Clean up classroom

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Cleanup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
