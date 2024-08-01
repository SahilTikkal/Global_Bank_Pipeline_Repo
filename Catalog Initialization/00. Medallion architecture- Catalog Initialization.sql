-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS Global_bank;
USE CATALOG global_bank;

-- COMMAND ----------

USE CATALOG global_bank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Schemas 

-- COMMAND ----------

USE CATALOG hive_metastore;

CREATE SCHEMA IF NOT EXISTS   hive_metastore.GB_bronze_schema;
CREATE SCHEMA IF NOT EXISTS   hive_metastore.GB_silver_schema;
CREATE SCHEMA IF NOT EXISTS   hive_metastore.GB_gold_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating tables for respected Layer in medallion architecture.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Bronze Tables

-- COMMAND ----------

-- DBTITLE 1,Customers table
CREATE TABLE IF NOT EXISTS hive_metastore.GB_bronze_schema.customers_raw(
  customer_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  credit_score INT,
  join_date DATE,
  last_update TIMESTAMP,
  customer_type STRING
)
USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Branches table

 CREATE TABLE IF NOT EXISTS GB_bronze_schema.branches_raw(
  branch_id STRING,
  branch_name STRING,
  location STRING,
  timezone STRING,
  currency STRING
)
 USING DELTA

-- COMMAND ----------

-- DBTITLE 1,Transactions table
CREATE TABLE IF NOT EXISTS GB_bronze_schema.transactions_raw(
    transaction_id STRING,
    customer_id STRING,
    branch_id STRING,
    channel STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING,
    status STRING
)
USING DELTA

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver tables

-- COMMAND ----------

-- DBTITLE 1,Cleaned and Transformed customer Table
-- Cleaned and Transformed customer Table
CREATE TABLE IF NOT EXISTS hive_metastore.gb_silver_schema.cat_customers (
    customer_id STRING,
    name STRING,
    email STRING,
    phone BIGINT,
    address STRING,
    credit_score INT,
    join_date DATE,
    last_update STRING,
    customer_type STRING,
    year INT,
    domain STRING,
    postal_code INT
)
USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,Cleaned and Transformed Branch Table
-- Cleaned and Transformed Branch Table
CREATE TABLE IF NOT EXISTS hive_metastore.gb_silver_schema.cat_branches (
    branch_id STRING,
    branch_name STRING,
    location STRING,
    timezone STRING,
    currency STRING,
    latitude DOUBLE,
    longitude DOUBLE
)
USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,Cleaned and Transformed Transactions Table
-- Cleaned and Transformed Transactions Table

CREATE TABLE IF NOT EXISTS hive_metastore.gb_silver_schema.cat_transactions (
    transaction_id STRING,
    customer_id STRING,
    branch_id STRING,
    channel STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING,
    status STRING,
    year INT,
    month INT,
    Amount_USD DOUBLE
)
USING DELTA;

