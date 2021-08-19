# Databricks notebook source
dbutils.widgets.text("employee",",")
employee="/mnt/blob-mount/source/employee.txt"+dbutils.widgets.get("employee")
df=spark.read.option("header","true").option("inferSchema","true").csv(employee)
display(df)

# COMMAND ----------


