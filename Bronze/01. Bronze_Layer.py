# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql.types import *
import json

# COMMAND ----------

# DBTITLE 1,Running configuration notebook
# MAGIC %run "/Workspace/Users/hemantkhorwal99@gmail.com/Devlopment/Config/GB_Project_Config"

# COMMAND ----------

# DBTITLE 1,Creating Checkpointing and schema folder for streaming
"""
    This code is to create and delete checkpointing and schemas
    uncomment if needed
"""
# dbutils.fs.rm("/FileStore/Checkpoints",True)
# dbutils.fs.rm("/FileStore/Streaming_Schema",True)
# dbutils.fs.mkdirs("/FileStore/Checkpoints/Bronze/Transactions")
# dbutils.fs.mkdirs("/FileStore/Checkpoints/Silver/Transactions")
# dbutils.fs.mkdirs("/FileStore/Checkpoints/Gold/Transactions")
# dbutils.fs.mkdirs("/FileStore/Streaming_Schema/Bronze/Transactions")
# dbutils.fs.mkdirs("/FileStore/Streaming_Schema/Silver/Transactions")
# dbutils.fs.mkdirs("/FileStore/Streaming_Schema/Gold/Transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC #Bronza Data ingestion 

# COMMAND ----------

# MAGIC %md
# MAGIC ## External Location Path

# COMMAND ----------

# DBTITLE 1,Reading configuration file
# Function to read the JSON configuration file
# def read_config(config_file):
#     with open(config_file, 'r') as file:
#         config = json.load(file)
#     return config

# # Use the configuration data
# config_file = '/Workspace/Users/hemantkhorwal99@gmail.com/Devlopment/Config/config_DataBricks.json'
# config = read_config(config_file)

# COMMAND ----------

# DBTITLE 1,Setting up ADLS storage
storage_account = config["adls_connection"]["storage_account"]
container_name = config["adls_connection"]["container_name"]
storage_account_conf_key = f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
storage_account_conf_key_value = config["adls_connection"]["access_key"]

spark.conf.set(storage_account_conf_key,storage_account_conf_key_value)

bank_ext_location = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Ingestion from adls to bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load the data --> Customer table

# COMMAND ----------

# file path in adls
file_names = config["paths"]['customers']
branch_dataset_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_names}"

# Reading files
customer_df = spark.read.csv(path=branch_dataset_path,header=True, schema=customer_schema)
customer_df.display()
print(f"Total no of records:{customer_df.count()}")

# Saving  raw data to Customer Table
customer_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_bronze_schema.customers_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Load the data --> Branch table (static Data table)

# COMMAND ----------


# file path in adls
file_names = config["paths"]['branch']
branch_dataset_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_names}"

# Reading files
branch_df = spark.read.csv(path=branch_dataset_path,header=True, schema=branches_schema)
branch_df.display()
print(f"Total no of records:{branch_df.count()}")

# Saving data to Branch Table
branch_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_bronze_schema.branches_raw")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Load the data which is streaming tables - Transactions Table

# COMMAND ----------

# DBTITLE 1,Streaming table - transactions table - without config
# # file path in adls
# file_names = config["paths"]["transactions"]
# transaction_dataset_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_names}"

# def stream_transaction_table():
#     source_df = (spark.readStream
#                         .format("cloudFiles")
#                         .option("cloudFiles.format", "csv")  
#                         .option("header", "true") 
#                         .option("timestampFormat","yyyy-MM-dd")
#                         .schema(transactions_schema)  # Explicit schema
#                         .option("cloudFiles.schemaLocation", "dbfs:/FileStore/Streaming_Schema")
#                         .option("cloudFiles.inferColumnTypes", "true")                    
#                         .load(transaction_dataset_path)
#     )

#     write_query = (source_df.writeStream
#                             .format("delta")
#                             .option("checkpointLocation", "dbfs:/FileStore/Checkpoints/transactions")
#                             .option("mergeSchema", "true")
#                             .outputMode("append")                         
#                             .trigger(processingTime='30 seconds')  # Adjust as needed
#                             .toTable("hive_metastore.gb_bronze_schema.transactions_Streaming")
#     )

#     return write_query

# COMMAND ----------

# DBTITLE 1,Streaming table - transactions table

# File path in ADLS
file_names = config["paths"]["transactions"]
container_name = config["adls_connection"]["container_name"]
storage_account = config["adls_connection"]["storage_account"]
transaction_dataset_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_names}"

def stream_transaction_table():
    source_df = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", config["cloudFiles"]["format"])  
                        .option("header", config["cloudFiles"]["header"]) 
                        .option("timestampFormat", config["cloudFiles"]["timestampFormat"])
                        .schema(transactions_schema)  # Explicit schema
                        .option("cloudFiles.schemaLocation", config["cloudFiles"]["schemaLocation"])
                        .option("cloudFiles.inferColumnTypes", config["cloudFiles"]["inferColumnTypes"])                    
                        .load(transaction_dataset_path)
    )

    write_query = (source_df.writeStream
                            .format("delta")
                            .option("checkpointLocation", config["delta"]["checkpointLocation"])
                            .option("mergeSchema", config["delta"]["mergeSchema"])
                            .outputMode(config["delta"]["outputMode"])                         
                            .trigger(processingTime=config["delta"]["processingTime"])  # Adjust as needed
                            .toTable(config["delta"]["table"])
    )

    return write_query



# COMMAND ----------

# Call the stream_transaction_table function to start the streaming ingestion
stream_transaction_table()

# COMMAND ----------

# DBTITLE 1,Validating stream
# MAGIC %sql
# MAGIC select  * from hive_metastore.gb_bronze_schema.transactions_Streaming;

# COMMAND ----------

# DBTITLE 1,Truncating streaming table
# MAGIC %sql
# MAGIC /*
# MAGIC     - Use this to truncate streaming table in case of restarting
# MAGIC     - uncomment necessary command
# MAGIC */
# MAGIC
# MAGIC -- truncate table hive_metastore.gb_bronze_schema.transactions_Streaming;