# Databricks notebook source
# MAGIC %python
# MAGIC #############################################
# MAGIC # TAG API FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # Get all tags
# MAGIC def getTags() -> dict:
# MAGIC   return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
# MAGIC     dbutils.entry_point.getDbutils().notebook().getContext().tags()
# MAGIC   )
# MAGIC 
# MAGIC # Get a single tag's value
# MAGIC def getTag(tagName: str, defaultValue: str = None) -> str:
# MAGIC   values = getTags()[tagName]
# MAGIC   try:
# MAGIC     if len(values) > 0:
# MAGIC       return values
# MAGIC   except:
# MAGIC     return defaultValue
# MAGIC 
# MAGIC #############################################
# MAGIC # USER, USERNAME, AND USERHOME FUNCTIONS
# MAGIC #############################################
# MAGIC 
# MAGIC # Get the user's username
# MAGIC def getUsername() -> str:
# MAGIC   import uuid
# MAGIC   try:
# MAGIC     return dbutils.widgets.get("databricksUsername")
# MAGIC   except:
# MAGIC     return getTag("user", str(uuid.uuid1()).replace("-", ""))
# MAGIC 
# MAGIC # Get the user's userhome
# MAGIC def getUserhome() -> str:
# MAGIC   username = getUsername()
# MAGIC   return "dbfs:/user/{}".format(username)
# MAGIC 
# MAGIC def getModuleName() -> str:
# MAGIC   # This will/should fail if module-name is not defined in the Classroom-Setup notebook
# MAGIC   return spark.conf.get("com.databricks.training.module-name")
# MAGIC 
# MAGIC def getLessonName() -> str:
# MAGIC   # If not specified, use the notebook's name.
# MAGIC   return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]
# MAGIC 
# MAGIC def getWorkingDir() -> str:
# MAGIC   import re
# MAGIC   lessonName = re.sub("[^a-zA-Z0-9]", "_", getLessonName())
# MAGIC   moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName())
# MAGIC   userhome = getUserhome()
# MAGIC   return f"{userhome}/dbacademy/{moduleName}/{lessonName}/".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()
# MAGIC 
# MAGIC ############################################
# MAGIC # USER DATABASE FUNCTIONS
# MAGIC ############################################
# MAGIC 
# MAGIC def getDatabaseName(username:str, moduleName:str, lessonName:str) -> str:
# MAGIC   import re
# MAGIC   user = re.sub("[^a-zA-Z0-9]", "_", username)
# MAGIC   module = re.sub("[^a-zA-Z0-9]", "_", moduleName)
# MAGIC   lesson = re.sub("[^a-zA-Z0-9]", "_", lessonName)
# MAGIC   databaseName = f"dbacademy_{user}_{module}_{lesson}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").lower()
# MAGIC   return databaseName
# MAGIC 
# MAGIC 
# MAGIC # Create a user-specific database
# MAGIC def createUserDatabase(username:str, moduleName:str, lessonName:str) -> str:
# MAGIC   databaseName = getDatabaseName(username, moduleName, lessonName)
# MAGIC 
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
# MAGIC   spark.sql("USE {}".format(databaseName))
# MAGIC 
# MAGIC   return databaseName
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method to determine whether a path exists
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def pathExists(path):
# MAGIC   try:
# MAGIC     dbutils.fs.ls(path)
# MAGIC     return True
# MAGIC   except:
# MAGIC     return False
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method for recursive deletes
# MAGIC # Note: dbutils.fs.rm() does not appear to be truely recursive
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def deletePath(path):
# MAGIC   files = dbutils.fs.ls(path)
# MAGIC 
# MAGIC   for file in files:
# MAGIC     deleted = dbutils.fs.rm(file.path, True)
# MAGIC 
# MAGIC     if deleted == False:
# MAGIC       if file.is_dir:
# MAGIC         deletePath(file.path)
# MAGIC       else:
# MAGIC         raise IOError("Unable to delete file: " + file.path)
# MAGIC 
# MAGIC   if dbutils.fs.rm(path, True) == False:
# MAGIC     raise IOError("Unable to delete directory: " + path)
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Utility method to clean up the workspace at the end of a lesson
# MAGIC # ****************************************************************************
# MAGIC 
# MAGIC def classroomCleanup(username:str, moduleName:str, lessonName:str, dropDatabase:bool):
# MAGIC   import time
# MAGIC 
# MAGIC   # Stop any active streams
# MAGIC   if len(spark.streams.active) > 0:
# MAGIC     print(f"Stopping {len(spark.streams.active)} streams")
# MAGIC 
# MAGIC   for stream in spark.streams.active:
# MAGIC     try: 
# MAGIC       stream.stop()
# MAGIC       stream.awaitTermination()
# MAGIC     except: 
# MAGIC       pass # Bury any exceptions arising from stopping
# MAGIC 
# MAGIC   # Drop all tables from the specified database
# MAGIC   database = getDatabaseName(username, moduleName, lessonName)
# MAGIC   try:
# MAGIC     tables = spark.sql("show tables from {}".format(database)).select("tableName").collect()
# MAGIC     for row in tables:
# MAGIC       tableName = row["tableName"]
# MAGIC       spark.sql("drop table if exists {}.{}".format(database, tableName))
# MAGIC 
# MAGIC       # In some rare cases the files don't actually get removed.
# MAGIC       time.sleep(1) # Give it just a second...
# MAGIC       hivePath = "dbfs:/user/hive/warehouse/{}.db/{}".format(database, tableName)
# MAGIC       dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure
# MAGIC   except:
# MAGIC     pass # ignored
# MAGIC 
# MAGIC   # Remove any files that may have been created from previous runs
# MAGIC   path = getWorkingDir()
# MAGIC   if pathExists(path):
# MAGIC     deletePath(path)
# MAGIC 
# MAGIC   # The database should only be dropped in a "cleanup" notebook, not "setup"
# MAGIC   if dropDatabase:
# MAGIC     spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))
# MAGIC 
# MAGIC     # In some rare cases the files don't actually get removed.
# MAGIC     time.sleep(1) # Give it just a second...
# MAGIC     hivePath = "dbfs:/user/hive/warehouse/{}.db".format(database)
# MAGIC     dbutils.fs.rm(hivePath, True) # Ignoring the delete's success or failure
# MAGIC 
# MAGIC     displayHTML("Dropped database and removed files in working directory")
# MAGIC 
# MAGIC 
# MAGIC # Utility method to delete a database
# MAGIC def deleteTables(database):
# MAGIC   spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database))
# MAGIC 
# MAGIC # ****************************************************************************
# MAGIC # Placeholder variables for coding challenge type specification
# MAGIC # ****************************************************************************
# MAGIC class FILL_IN:
# MAGIC   from pyspark.sql.types import Row, StructType
# MAGIC   VALUE = None
# MAGIC   LIST = []
# MAGIC   SCHEMA = StructType([])
# MAGIC   ROW = Row()
# MAGIC   INT = 0
# MAGIC   DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))
# MAGIC 
# MAGIC ############################################
# MAGIC # Set up student environment
# MAGIC ############################################
# MAGIC 
# MAGIC moduleName = getModuleName()
# MAGIC username = getUsername()
# MAGIC lessonName = getLessonName()
# MAGIC userhome = getUserhome()
# MAGIC workingDir = getWorkingDir()
# MAGIC databaseName = createUserDatabase(username, moduleName, lessonName)
# MAGIC 
# MAGIC classroomCleanup(username, moduleName, lessonName, False)

# COMMAND ----------

# MAGIC %scala
# MAGIC //*******************************************
# MAGIC // TAG API FUNCTIONS
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get all tags
# MAGIC def getTags(): Map[com.databricks.logging.TagDefinition,String] = {
# MAGIC   com.databricks.logging.AttributionContext.current.tags
# MAGIC }
# MAGIC 
# MAGIC // Get a single tag's value
# MAGIC def getTag(tagName: String, defaultValue: String = null): String = {
# MAGIC   val values = getTags().collect({ case (t, v) if t.name == tagName => v }).toSeq
# MAGIC   values.size match {
# MAGIC     case 0 => defaultValue
# MAGIC     case _ => values.head.toString
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC //*******************************************
# MAGIC // USER, USERNAME, AND USERHOME FUNCTIONS
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get the user's username
# MAGIC def getUsername(): String = {
# MAGIC   return try {
# MAGIC     dbutils.widgets.get("databricksUsername")
# MAGIC   } catch {
# MAGIC     case _: Exception => getTag("user", java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // Get the user's userhome
# MAGIC def getUserhome(): String = {
# MAGIC   val username = getUsername()
# MAGIC   return s"dbfs:/user/$username"
# MAGIC }
# MAGIC 
# MAGIC def getModuleName(): String = {
# MAGIC   // This will/should fail if module-name is not defined in the Classroom-Setup notebook
# MAGIC   return spark.conf.get("com.databricks.training.module-name")
# MAGIC }
# MAGIC 
# MAGIC def getLessonName(): String = {
# MAGIC   // If not specified, use the notebook's name.
# MAGIC   return dbutils.notebook.getContext.notebookPath.get.split("/").last
# MAGIC }
# MAGIC 
# MAGIC def getWorkingDir(): String = {
# MAGIC   val lessonName = getLessonName().replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   val moduleName = getModuleName().replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   val userhome = getUserhome()
# MAGIC   return f"${userhome}/dbacademy/${moduleName}/${lessonName}".replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_").toLowerCase()
# MAGIC }
# MAGIC 
# MAGIC //**********************************
# MAGIC // USER DATABASE FUNCTIONS
# MAGIC //**********************************
# MAGIC 
# MAGIC def getDatabaseName(username:String, moduleName:String, lessonName:String):String = {
# MAGIC   val user = username.replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   val module = moduleName.replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   val lesson = lessonName.replaceAll("[^a-zA-Z0-9]", "_")
# MAGIC   val databaseName = f"dbacademy_${user}_${module}_${lesson}".toLowerCase
# MAGIC   return databaseName
# MAGIC }
# MAGIC 
# MAGIC // Create a user-specific database
# MAGIC def createUserDatabase(username:String, moduleName:String, lessonName:String):String = {
# MAGIC   val databaseName = getDatabaseName(username, moduleName, lessonName)
# MAGIC 
# MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS %s".format(databaseName))
# MAGIC   spark.sql("USE %s".format(databaseName))
# MAGIC 
# MAGIC   return databaseName
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to determine whether a path exists
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def pathExists(path:String):Boolean = {
# MAGIC   try {
# MAGIC     dbutils.fs.ls(path)
# MAGIC     return true
# MAGIC   } catch{
# MAGIC     case e: Exception => return false
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method for recursive deletes
# MAGIC // Note: dbutils.fs.rm() does not appear to be truely recursive
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def deletePath(path:String):Unit = {
# MAGIC   val files = dbutils.fs.ls(path)
# MAGIC 
# MAGIC   for (file <- files) {
# MAGIC     val deleted = dbutils.fs.rm(file.path, true)
# MAGIC 
# MAGIC     if (deleted == false) {
# MAGIC       if (file.isDir) {
# MAGIC         deletePath(file.path)
# MAGIC       } else {
# MAGIC         throw new java.io.IOException("Unable to delete file: " + file.path)
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   if (dbutils.fs.rm(path, true) == false) {
# MAGIC     throw new java.io.IOException("Unable to delete directory: " + path)
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to clean up the workspace at the end of a lesson
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def classroomCleanup(username:String, moduleName:String, lessonName:String, dropDatabase: Boolean):Unit = {
# MAGIC 
# MAGIC   // Stop any active streams
# MAGIC   for (stream <- spark.streams.active) {
# MAGIC     try { 
# MAGIC       stream.stop()
# MAGIC       stream.awaitTermination()
# MAGIC     } catch { 
# MAGIC       // Bury any exceptions arising from stopping the stream
# MAGIC       case _: Exception => () 
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   // Drop the tables only from specified database
# MAGIC   val database = getDatabaseName(username, moduleName, lessonName)
# MAGIC   try {
# MAGIC     val tables = spark.sql(s"show tables from $database").select("tableName").collect()
# MAGIC     for (row <- tables){
# MAGIC       var tableName = row.getAs[String]("tableName")
# MAGIC       spark.sql("drop table if exists %s.%s".format(database, tableName))
# MAGIC 
# MAGIC       // In some rare cases the files don't actually get removed.
# MAGIC       Thread.sleep(1000) // Give it just a second...
# MAGIC       val hivePath = "dbfs:/user/hive/warehouse/%s.db/%s".format(database, tableName)
# MAGIC       dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
# MAGIC 
# MAGIC     }
# MAGIC   } catch {
# MAGIC     case _: Exception => () // ignored
# MAGIC   }
# MAGIC 
# MAGIC   // Remove files created from previous runs
# MAGIC   val path = getWorkingDir()
# MAGIC   if (pathExists(path)) {
# MAGIC     deletePath(path)
# MAGIC   }
# MAGIC 
# MAGIC   // Drop the database if instructed to
# MAGIC   if (dropDatabase){
# MAGIC     spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
# MAGIC 
# MAGIC     // In some rare cases the files don't actually get removed.
# MAGIC     Thread.sleep(1000) // Give it just a second...
# MAGIC     val hivePath = "dbfs:/user/hive/warehouse/%s.db".format(database)
# MAGIC     dbutils.fs.rm(hivePath, true) // Ignoring the delete's success or failure
# MAGIC 
# MAGIC     displayHTML("Dropped database and removed files in working directory")
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Utility method to delete a database
# MAGIC // ****************************************************************************
# MAGIC 
# MAGIC def deleteTables(database:String):Unit = {
# MAGIC   spark.sql("DROP DATABASE IF EXISTS %s CASCADE".format(database))
# MAGIC }
# MAGIC 
# MAGIC // ****************************************************************************
# MAGIC // Placeholder variables for coding challenge type specification
# MAGIC // ****************************************************************************
# MAGIC object FILL_IN {
# MAGIC   val VALUE = null
# MAGIC   val ARRAY = Array(Row())
# MAGIC   val SCHEMA = org.apache.spark.sql.types.StructType(List())
# MAGIC   val ROW = Row()
# MAGIC   val LONG: Long = 0
# MAGIC   val INT: Int = 0
# MAGIC   def DATAFRAME = spark.emptyDataFrame
# MAGIC   def DATASET = spark.createDataset(Seq(""))
# MAGIC }
# MAGIC 
# MAGIC //**********************************
# MAGIC // Set up student environment
# MAGIC //**********************************
# MAGIC 
# MAGIC val moduleName = getModuleName()
# MAGIC val lessonName = getLessonName()
# MAGIC val username = getUsername()
# MAGIC val userhome = getUserhome()
# MAGIC val workingDir = getWorkingDir()
# MAGIC val databaseName = createUserDatabase(username, moduleName, lessonName)
# MAGIC 
# MAGIC classroomCleanup(username, moduleName, lessonName, false)
# MAGIC 
# MAGIC displayHTML("")
