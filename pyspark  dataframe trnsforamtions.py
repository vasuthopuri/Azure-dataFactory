# Databricks notebook source
# MAGIC %md
# MAGIC Cheking the mount in databricks 

# COMMAND ----------


dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/adls/source/sample_data.txt'

# COMMAND ----------

# MAGIC %md
# MAGIC Reading data from Mount point in adls  and converting integer data type to String 

# COMMAND ----------

df1=spark.read.csv("dbfs:/mnt/adls/source/employee.txt",header=True,inferSchema=True)
df2=df1.withColumn("emp_no",df1.emp_no.cast("string"))



# COMMAND ----------

# MAGIC %md
# MAGIC #Removing $ sign from price cloumn by using replace and expr 

# COMMAND ----------

from pyspark.sql.functions import expr
df3=spark.read.csv("dbfs:/mnt/adls/source/sample_data.txt",header=True,inferSchema=True)
df4=df3.withColumn("price", expr("replace(price, '$', '')")).show()
display(df4)



# COMMAND ----------


