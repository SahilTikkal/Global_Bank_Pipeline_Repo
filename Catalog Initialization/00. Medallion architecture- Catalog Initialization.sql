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
-- MAGIC ## 2. Silver tables

-- COMMAND ----------

-- DBTITLE 1,Creating silver table with cleaned data
CREATE TABLE IF NOT EXISTS ORS_silver_schema.ORS_silver (
  InvoiceNo STRING,
  StockCode string,
  Description string,
  Quantity int,
  InvoiceDate timestamp,
  UnitPrice double,
  CustomerID int,
  Country string,
  InvoiceDateOnly date,
  Total_sales double
)
USING DELTA PARTITIONED BY (InvoiceDateOnly)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Gold layer Table

-- COMMAND ----------

-- DBTITLE 1,Daily Summary table
CREATE TABLE ORS_gold_schema.DailySalesSummary (
    InvoiceDateOnly DATE,
    total_daily_sales DOUBLE,
    total_daily_sales_avg DOUBLE,
    daily_total_quantity BIGINT,
    daily_quantity_avg DOUBLE,
    daily_total_UnitPrice DOUBLE,
    Daily_UnitPrice_avg DOUBLE
)
 USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,product summary table
CREATE TABLE ProductSalesSummary (
    StockCode STRING,
    total_sales DOUBLE,
    total_sales_avg DOUBLE,
    total_quantity BIGINT,
    quantity_avg DOUBLE,
    total_UnitPrice DOUBLE,
    UnitPrice_avg DOUBLE
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Customer Summary Table
CREATE TABLE CustomerSalesSummary (
    CustomerID INT,
    total_sales DOUBLE,
    total_sales_avg DOUBLE,
    total_quantity BIGINT,
    quantity_avg DOUBLE,
    total_UnitPrice DOUBLE,
    UnitPrice_avg DOUBLE
)
USING DELTA;

-- COMMAND ----------

