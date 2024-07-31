# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Customer Table Schema
# Define schema for Customers table
customer_schema =StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("join_date", DateType(), True),
    StructField("last_update", TimestampType(), True),
    StructField("customer_type", StringType(), True)
]) 


# COMMAND ----------

# DBTITLE 1,Branch Table Schema
# Define schema for Branch table
branches_schema = StructType([
    StructField("branch_id", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("currency", StringType(), True)
]) 


# COMMAND ----------

# DBTITLE 1,Transaction Table Schema
# Define schema for transaction data
transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

config = {
  "adls_connection" :{
    "storage_account" : "mavericstoragecapstone",
    "container_name" : "global-bank",
    "access_key" : "NOTallowedonGIThub",
    "storage_account_conf_key" : "fs.azure.account.key.mavericstoragecapstone.dfs.core.windows.net",
    "ingestion_location" : "abfss://global-bank@mavericstoragecapstone.dfs.core.windows.net/"
  },
  "paths":{
    "customers" : "/Transactions/csv/Customers.csv",
    "branch" : "/Transactions/csv/Branches.csv",
    "transactions" : "/Transactions/Transactions/"
  },

  "bronze_stream": {
    "cloudFiles": {
        "format": "csv",
        "header": "true",
        "timestampFormat": "yyyy-MM-dd",
        "schemaLocation": "dbfs:/FileStore/Streaming_Schema/Bronze/Transactions",
        "inferColumnTypes": "true"
    },
    "delta": {
        "checkpointLocation": "dbfs:/FileStore/Checkpoints/Bronze/Transactions",
        "mergeSchema": "true",
        "outputMode": "append",
        "processingTime": "30 seconds",
        "table": "hive_metastore.gb_bronze_schema.transactions_Streaming"
    }
  }


}