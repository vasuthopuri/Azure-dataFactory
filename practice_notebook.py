# Databricks notebook source
Mounting for ADLS 

# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
# "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#  #app regestration client id from appregestration("ee654cc8-2b59-4c4a-9bae-7a33b8a02df7")
# "fs.azure.account.oauth2.client.id": "ee654cc8-2b59-4c4a-9bae-7a33b8a02df7",
#  #Scope in databricks key=appregstration value key(ADLSaccesskey)
# "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adls_mnt",key="ADLSaccesskey"),
#  #appregestration tentid ("274788b6-6ef5-4229-802c-5b3b222fd082")
# "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/274788b6-6ef5-4229-802c-5b3b222fd082/oauth2/token"} 

# # Optionally, you can add <your-directory> to the URL of your mount point. 

# dbutils.fs.mount(
#   # source "abfss://container name@astorage account name.dfs.core.windows.net"
#   source = "abfss://adlsstorage@adlsstorageexamp.dfs.core.windows.net",
#   #mount name 
#   mount_point = "/mnt/adls",
#   extra_configs = configs) 

# COMMAND ----------

mounting for blob storage

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://blob-mount@blobstoragemount.blob.core.windows.net",
#   mount_point = "/mnt/blob-mount",
#   extra_configs = {"fs.azure.account.key.blobstoragemount.blob.core.windows.net":dbutils.secrets.get(scope = "adls_mnt", key = "blobaccesskey")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/blob-mount/source/"

# COMMAND ----------

df1=spark.read.csv("dbfs:/mnt/blob-mount/source/examp.txt",header=True,inferSchema=True)
df2=spark.read.csv("dbfs:/mnt/blob-mount/source/examp2.txt",header=True,inferSchema=True)
df3=df2.withColumnRenamed("b","a")
df4=df1.join(df3,on=["a"]  , how = 'right')
display(df4)


# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/adls/source/"

# COMMAND ----------

df=spark.read.csv("/mnt/adls/source/employee.txt",header=True,inferSchema=False)
df.display()

# COMMAND ----------


