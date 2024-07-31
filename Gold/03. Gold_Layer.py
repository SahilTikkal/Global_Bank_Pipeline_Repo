# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import json
import requests
import time
from urllib.parse import quote
import pandas as pd
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC # Import Cleaned and Transformed Tables from Silver Layer
# MAGIC 1. customers table
# MAGIC 2. branch table
# MAGIC 3. transaction table

# COMMAND ----------

# DBTITLE 1,Importing Silver tables
customer_df = spark.read.table("hive_metastore.gb_silver_schema.CAT_customers")
branch_df = spark.read.table("hive_metastore.gb_silver_schema.CAT_Branches")
transaction_df = spark.read.table("hive_metastore.gb_silver_schema.CAT_Transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Customer Segmentation table and fraud flag table

# COMMAND ----------

# DBTITLE 1,FN: Data Summery Table
from pyspark.sql.types import NumericType
def data_summary(df: DataFrame):
    summary = {}

    # Count of total rows
    total_count = df.count()
    summary["total_count"] = total_count

    # Loop through each column and gather summary statistics
    for col_name in df.columns:
        summary[col_name] = {}

        # Count of non-null values
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        summary[col_name]["non_null_count"] = non_null_count

        # Count of unique values
        unique_count = df.select(countDistinct(col(col_name)).alias("count")).collect()[0]["count"]
        summary[col_name]["unique_count"] = unique_count

        # If the column is numeric, get additional statistics
        if isinstance(df.schema[col_name].dataType, NumericType):
            numeric_summary = df.select(
                mean(col(col_name)).alias("mean"),
                stddev(col(col_name)).alias("stddev"),
                min(col(col_name)).alias("min"),
                max(col(col_name)).alias("max")
            ).collect()[0]
            summary[col_name].update({
                "mean": numeric_summary["mean"],
                "stddev": numeric_summary["stddev"],
                "min": numeric_summary["min"],
                "max": numeric_summary["max"]
            })

    # Print summary in a readable format
    print(f"Total Rows: {total_count}\n")
    for col_name, stats in summary.items():
        if col_name != "total_count":
            print("==o"*10)
            print(f"Column: {col_name}")
            print(f"  Non-Null Count: {stats['non_null_count']}")
            print(f"  Null count:  {total_count - (stats['non_null_count'])}")
            print(f"  Unique Count: {stats['unique_count']}")
            if "mean" in stats:
                print(f"  Mean: {stats['mean']}")
                print(f"  StdDev: {stats['stddev']}")
                print(f"  Min: {stats['min']}")
                print(f"  Max: {stats['max']}")
            print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating Customer_Segments table

# COMMAND ----------

# DBTITLE 1,FN: Customer segmetation  functions
# Fixed current date
current_date = pd.Timestamp("2023-03-01")

# High_Value: Customers with high transaction volume
high_value_threshold = 100000  # Define a threshold for high transaction volume
high_value_customers = transaction_df.groupBy("customer_id").agg(
    sum("amount").alias("total_amount")
)
high_value_customers = (
    high_value_customers.filter(col("total_amount") > high_value_threshold)
    .select("customer_id")
    .withColumn("segment_name", lit("High_Value"))
    .withColumn("segment_description", lit("Customers with high transaction volume"))
    .withColumn("last_update", lit(current_date))
)

# New_User: Customers who joined in the last 30 days
new_user_customers = (
    customer_df.filter(col("join_date") > current_date - timedelta(days=90))
    .select("customer_id")
    .withColumn("segment_name", lit("New_User"))
    .withColumn("segment_description", lit("Customers who joined in last 30 days"))
    .withColumn("last_update", lit(current_date))
)

# Inactive: Customers with no transactions in last 90 days
recent_transactions = (
    transaction_df.filter(col("timestamp") > current_date - timedelta(days=10))
    .select("customer_id")
    .distinct()
)
inactive_customers = (
    customer_df.join(recent_transactions, on="customer_id", how="left_anti")
    .select("customer_id")
    .withColumn("segment_name", lit("Inactive"))
    .withColumn("segment_description", lit("No transactions in last 90 days"))
    .withColumn("last_update", lit(current_date))
)

# Credit_Risk: Customers with low credit scores (assuming credit_score < 600)
credit_risk_customers = (
    customer_df.filter(col("credit_score") < 600)
    .select("customer_id")
    .withColumn("segment_name", lit("Credit_Risk"))
    .withColumn("segment_description", lit("Customers with low credit scores"))
    .withColumn("last_update", lit(current_date))
)

# Loyal: Customers with consistent activity for over 5 years
loyal_customers = (
    customer_df.filter(col("join_date") < current_date - timedelta(days=5 * 365))
    .select("customer_id")
    .withColumn("segment_name", lit("Loyal"))
    .withColumn("segment_description", lit("Consistent activity for over 5 years"))
    .withColumn("last_update", lit(current_date))
)

# Combine all segments
customer_segmentation_df = (
    high_value_customers.union(new_user_customers)
    .union(inactive_customers)
    .union(credit_risk_customers)
    .union(loyal_customers)
)

# Add segment_id
window_spec = Window.orderBy("customer_id")
customer_segmentation_df = customer_segmentation_df.withColumn(
    "segment_id",
    concat(
        lit("S"), lpad((row_number().over(window_spec) + 1000).cast("string"), 4, "0")
    ),
)

# Show the result
customer_segmentation_df = customer_segmentation_df.select(
    "segment_id", "customer_id", "segment_name", "segment_description", "last_update"
)

# COMMAND ----------

display(customer_segmentation_df)

# COMMAND ----------

# DBTITLE 1,Customer_segmentation summery
data_summary(customer_segmentation_df)

# COMMAND ----------

# DBTITLE 1,Aggregating customer segmentation
from pyspark.sql.functions import *
segment_count_df = customer_segmentation_df.groupBy("segment_name").agg(count("segment_name").alias("count"))

# Show the result
segment_count_df.show()


# COMMAND ----------

# DBTITLE 1,Saving Customer_segmentation table
customer_segmentation_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_gold_schema.GB_customer_segmentation_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating Fraud_flag Table

# COMMAND ----------

# DBTITLE 1,FN: Functions to Detect fraud category
from pyspark.sql.functions import col, lit, when, concat, lpad, row_number
from pyspark.sql.window import Window

# Function to detect unusual amounts
def detect_unusual_amount(df):
    return df.withColumn(
        "unusual_amount",
        when(col("amount_usd") > 50000, lit(0.75)).otherwise(lit(None))
    )

# Function to detect velocity of transactions
def detect_velocity_check(df):
    monthly_counts = df.groupBy("year", "month", "customer_id") \
        .agg(count("transaction_id").alias("monthly_count"))
   
    df_with_counts = df.join(monthly_counts, on=["month", "year", "customer_id"], how="left")
   
    return df_with_counts.withColumn(
        "velocity_check",
        when(col("monthly_count") > 7, lit(0.60)).otherwise(lit(None))
    )

# Function to detect watchlist matches
def detect_watchlist_match(df):
    return df.withColumn(
        "watchlist_match",
        when(col("amount_usd") > 30000, lit(0.90)).otherwise(lit(None))
    )

# Function to detect pattern anomalies
def detect_pattern_anomaly(df):
    return df.withColumn(
        "pattern_anomaly",
        when(col("amount_usd") > 100000, lit(0.85)).otherwise(lit(None))
    )



# COMMAND ----------

# DBTITLE 1,Creating Fraud flag table
from pyspark.sql.functions import col, lit, when, concat, lpad, row_number, coalesce
from pyspark.sql.window import Window

# Apply fraud detection functions and ensure 'timestamp' column is included
transaction_df1 = detect_pattern_anomaly(transaction_df)
transaction_df2 = detect_unusual_amount(transaction_df)
transaction_df3 = detect_watchlist_match(transaction_df)
transaction_df4 = detect_velocity_check(transaction_df)

# Rename columns to avoid ambiguity
transaction_df1 = transaction_df1.withColumnRenamed("pattern_anomaly", "pattern_anomaly_1")
transaction_df2 = transaction_df2.withColumnRenamed("unusual_amount", "unusual_amount_2")
transaction_df3 = transaction_df3.withColumnRenamed("watchlist_match", "watchlist_match_3")
transaction_df4 = transaction_df4.withColumnRenamed("velocity_check", "velocity_check_4")

# Include 'timestamp' in the join keys to ensure it is retained
combined_df = transaction_df1 \
    .join(transaction_df2, on=['transaction_id', 'customer_id', 'year', 'month', 'timestamp'], how='outer') \
    .join(transaction_df3, on=['transaction_id', 'customer_id', 'year', 'month', 'timestamp'], how='outer') \
    .join(transaction_df4, on=['transaction_id', 'customer_id', 'year', 'month', 'timestamp'], how='outer')


# Coalesce and final selection
final_df = combined_df.withColumn(
    "pattern_anomaly", coalesce(col("pattern_anomaly_1"))
).withColumn(
    "unusual_amount", coalesce(col("unusual_amount_2"))
).withColumn(
    "watchlist_match", coalesce(col("watchlist_match_3"))
).withColumn(
    "velocity_check", coalesce(col("velocity_check_4"))
)

# Add 'flag_id' column starting from F1001
window_spec = Window.orderBy("transaction_id")  # Ensure the ordering column is present

fraud_flags_df = final_df.select(
    col("transaction_id"),
    col("customer_id"),
    col("month"),
    col("year"),
    when(col("pattern_anomaly").isNotNull(), lit("pattern_anomaly"))
    .when(col("unusual_amount").isNotNull(), lit("unusual_amount"))
    .when(col("watchlist_match").isNotNull(), lit("watchlist_match"))
    .when(col("velocity_check").isNotNull(), lit("velocity_check"))
    .alias("flag_type"),
    when(col("pattern_anomaly").isNotNull(), col("pattern_anomaly"))
    .when(col("unusual_amount").isNotNull(), col("unusual_amount"))
    .when(col("watchlist_match").isNotNull(), col("watchlist_match"))
    .when(col("velocity_check").isNotNull(), col("velocity_check"))
    .alias("confidence_score"),
    col("timestamp").alias("transaction_datetime")  # Ensure 'timestamp' is available
).filter(col("flag_type").isNotNull()) \
.withColumn("flag_id", concat(lit("F"), lpad((row_number().over(window_spec) + 1000).cast("string"), 4, "0")))

# Display the final DataFrame with fraud flags and flag_id
fraud_flags_df.display()


# COMMAND ----------

# DBTITLE 1,Saving Fraud_flag table
fraud_flags_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_gold_schema.GB_fraud_flags_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Aggreagations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers Aggregations

# COMMAND ----------

# DBTITLE 1,FN: Aggregating customer data
def aggregate_customers(customers_df, transactions_df):
    # Join customers_df with transactions_df on customer_id
    joined_df = customers_df.join(transactions_df, "customer_id")

    # Customer Segmentation
    customer_segmentation = joined_df.groupBy("customer_type") \
        .agg(
            F.countDistinct("customer_id").alias("total_customers"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.sum("amount").alias("total_spend")
        )

    # Find the latest transaction timestamp
    last_transaction_date = transactions_df.select(F.max("timestamp")).collect()[0][0]

    # Define a window to calculate the most recent transaction date for each customer
    window_spec = Window.partitionBy("customer_id")

    # Add a column with the most recent transaction date for each customer
    transactions_df = transactions_df.withColumn(
        "last_transaction_date", F.max("timestamp").over(window_spec)
    )

    # Identify active customers: transactions within the last 180 days
    active_customers_df = transactions_df.filter(
        F.datediff(F.lit(last_transaction_date), "last_transaction_date") <= 180
    ).select("customer_id").distinct()

    # Identify churned customers: no transactions in the last 365 days
    churned_customers_df = transactions_df.filter(
        F.datediff(F.lit(last_transaction_date), "last_transaction_date") > 180
    ).select("customer_id").distinct()

    # Get the counts
    active_customers = active_customers_df.count()
    churned_customers = churned_customers_df.count()

    return customer_segmentation, active_customers, churned_customers


# COMMAND ----------

# DBTITLE 1,Aggregating customers table data
customer_segmentation, active_customers, churned_customers= aggregate_customers(customer_df, transaction_df)

# COMMAND ----------

# DBTITLE 1,Output
display(customer_segmentation)
print("  The number of active users: ",active_customers)
print("  The number of inactive users: ",churned_customers)

# COMMAND ----------

# DBTITLE 1,Aggregation Transactions amount in USD by Customer
customer_aggregations_df = transaction_df.groupBy("customer_id").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

customer_aggregations_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch Data Aggregations

# COMMAND ----------

# DBTITLE 1,FN: Branch Performance and Analysis
def Currency_Analysis(transactions_df):
      # Currency Analysis
    currency_analysis = transactions_df.groupBy("currency") \
        .agg(
            F.sum("Amount_USD").alias("total_amount_by_currency"),
            F.avg("Amount_USD").alias("avg_amount_by_currency")
        )

    return currency_analysis


# COMMAND ----------

# DBTITLE 1,Aggregating branch data
currency_analysis = Currency_Analysis(transaction_df)

# COMMAND ----------

# DBTITLE 1,Output
display(currency_analysis)

# COMMAND ----------

# DBTITLE 1,Aggregation Transactions by Branch
# Aggregations by Branch with Amounts
branch_aggregations_df = transaction_df.groupBy("branch_id").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

branch_aggregations_df.display()

# COMMAND ----------

# DBTITLE 1,Aggregations by Location with Amounts
transaction_with_location_df = transaction_df.join(
    branch_df.select("branch_id", "location", "timezone"),
    on="branch_id",
    how="left"
)


# Aggregations by Location with Rounded Amounts
location_aggregations_df = transaction_with_location_df.groupBy("location").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

location_aggregations_df.display()

# COMMAND ----------

# DBTITLE 1,Aggregations by Time zone with Amounts
timezone_aggregations_df = transaction_with_location_df.groupBy("timezone").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

timezone_aggregations_df.display()

# COMMAND ----------

# DBTITLE 1,Aggregations by Currency with Amounts
# Aggregations by Currency with Amounts
currency_aggregations_df = transaction_df.groupBy("currency").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

currency_aggregations_df.display()


# COMMAND ----------

transaction_df.printSchema()
branch_df.printSchema()
customer_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction table aggregations

# COMMAND ----------

# DBTITLE 1,Channel by Amount in USD
channel_aggregations_df = transaction_df.groupBy("channel").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)
display(channel_aggregations_df)

# COMMAND ----------

# DBTITLE 1,Transaction Type with by Amount in USD
# Aggregations by Transaction Type with Rounded Amounts
transaction_type_aggregations_df = transaction_df.groupBy("transaction_type").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

transaction_type_aggregations_df.display()


# COMMAND ----------

# DBTITLE 1,Year and Month with by Amount in USD
# Aggregations by Year and Month with Amounts
year_month_aggregations_df = transaction_df.groupBy("year", "month").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

year_month_aggregations_df.display()


# COMMAND ----------

# creating date column
transaction_df_with_date = transaction_df.withColumn("date", to_date("timestamp"))

# Aggregations by Year and Month with Amounts
date_aggregations_df = transaction_df_with_date.groupBy("date").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

date_aggregations_df.display()

# COMMAND ----------

# DBTITLE 1,Status with Amounts in USD
# Aggregations by Status with Rounded Amounts
status_aggregations_df = transaction_df.groupBy("status").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

status_aggregations_df.display()


# COMMAND ----------

# DBTITLE 1,Channel and Transaction by by Amount in USD
# Aggregations by Channel and Transaction Type with Rounded Amounts
channel_type_aggregations_df = transaction_df.groupBy("channel", "transaction_type").agg(
    round(sum("Amount_USD"), 2).alias("total_amount"),
    round(avg("Amount_USD"), 2).alias("average_amount"),
    min("Amount_USD").alias("min_amount"),
    max("Amount_USD").alias("max_amount"),
    count("Amount_USD").alias("transaction_count")
)

channel_type_aggregations_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Aggregation

# COMMAND ----------

# DBTITLE 1,customer segments based on total transaction amount
# Define customer segments based on total transaction amount
customer_segments_df = transaction_df.groupBy("customer_id").agg(
    round(sum("Amount_USD"), 2).alias("total_amount_usd")
).withColumn(
    "segment",
    expr("CASE WHEN total_amount_usd <= 1000 THEN 'Low Value' "
         "WHEN total_amount_usd <= 5000 THEN 'Medium Value' "
         "ELSE 'High Value' END")
)

customer_segments_df.display()


# COMMAND ----------

# DBTITLE 1,customer retention and churn
# Define churn based on inactivity period
from pyspark.sql.functions import datediff, current_date

# Calculate last transaction date for each customer
last_transaction_df = transaction_df.groupBy("customer_id").agg(
    max("timestamp").alias("last_transaction_date")
).withColumn("current_date", lit(transaction_df.select(F.max("timestamp")).collect()[0][0]))

# Calculate inactivity period and churn status
churn_df = last_transaction_df.withColumn(
    "days_since_last_transaction", datediff(col("current_date"), "last_transaction_date")
).withColumn(
    "churn_status", expr("CASE WHEN days_since_last_transaction > 180 THEN 'Churned' ELSE 'Active' END")
)

churn_df.display()



# COMMAND ----------

# DBTITLE 1,Peak and Off-Peak Analysis
# Extract hour from timestamp
from pyspark.sql.functions import hour

# Define peak and off-peak hours
peak_hours = (8, 18)  # e.g., 8 AM to 6 PM

# Create a column for peak/off-peak classification
peak_off_peak_df = transaction_df.withColumn(
    "time_period",
    expr(f"CASE WHEN hour(timestamp) BETWEEN {peak_hours[0]} AND {peak_hours[1]} THEN 'Peak' ELSE 'Off-Peak' END")
).groupBy("time_period").agg(
    round(sum("Amount_USD"), 2).alias("total_amount_usd"),
    round(avg("Amount_USD"), 2).alias("average_amount_usd"),
    count("Amount_USD").alias("transaction_count")
)

peak_off_peak_df.display()


# COMMAND ----------

# DBTITLE 1,Transaction Frequency Distribution
# Define bins for transaction amount ranges
bins = [0, 50, 100, 500, 1000, 5000, 10000, float('inf')]
labels = ['0-50', '50-100', '100-500', '500-1000', '1000-5000', '5000-10000', '10000+']

# Create a new column for amount range
from pyspark.sql.functions import expr

transaction_frequency_df = transaction_df.withColumn(
    "amount_range",
    expr(f"CASE "
         f"WHEN Amount_USD <= {bins[1]} THEN '{labels[0]}' "
         f"WHEN Amount_USD <= {bins[2]} THEN '{labels[1]}' "
         f"WHEN Amount_USD <= {bins[3]} THEN '{labels[2]}' "
         f"WHEN Amount_USD <= {bins[4]} THEN '{labels[3]}' "
         f"WHEN Amount_USD <= {bins[5]} THEN '{labels[4]}' "
         f"WHEN Amount_USD <= {bins[6]} THEN '{labels[5]}' "
         f"ELSE '{labels[6]}' "  # This line replaces the check against float('inf')
         f"END AS amount_range")
).groupBy("amount_range").count().alias("transaction_count") 

transaction_frequency_df.display()


# COMMAND ----------

# DBTITLE 1,Median Transaction Amount
from pyspark.sql.functions import expr

# Calculate the median transaction amount by channel
median_df = transaction_df.groupBy("channel").agg(
    expr('percentile_approx(Amount_USD, 0.5)').alias('median_amount_usd')
)

median_df.show()


# COMMAND ----------

# DBTITLE 1,Percentage of Total
# Calculate total amount USD
total_amount = transaction_df.agg(sum("Amount_USD")).collect()[0][0]

# Calculate percentage of total amount by channel
percentage_df = transaction_df.groupBy("channel").agg(
    round(sum("Amount_USD"), 2).alias("total_amount_usd")
).withColumn("percentage_of_total", round((col("total_amount_usd") / total_amount) * 100, 2))

percentage_df.show()


# COMMAND ----------

# DBTITLE 1,Cumulative Sums
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as pyspark_sum


# Define WindowSpec for cumulative aggregation
cumulative_window_spec = Window.partitionBy("channel").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate cumulative sum
df_with_cumulative_sum = transaction_df.withColumn(
    "cumulative_sum", pyspark_sum("Amount_USD").over(cumulative_window_spec)
)

df_with_cumulative_sum.show()


# COMMAND ----------

# DBTITLE 1,Rolling Averages
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

# Define a window specification for rolling average (e.g., 7-day window)
window_spec = Window.partitionBy("channel").orderBy("timestamp").rowsBetween(-6, 0)

# Calculate 7-day rolling average of Amount_USD
rolling_avg_df = transaction_df.withColumn("rolling_avg_amount_usd", round(avg("Amount_USD").over(window_spec),2))

rolling_avg_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC ---
# MAGIC

# COMMAND ----------

# DBTITLE 1,Finding all the dataframes from notebook
from pyspark.sql import DataFrame

# List all DataFrame variables in the current namespace
dataframes = {name: obj for name, obj in globals().items() if isinstance(obj, DataFrame)}

# Print the names of all available DataFrames
for name in dataframes:
    print(name)


# COMMAND ----------

# DBTITLE 1,Saving Aggregations to tables
excluded_dataframes = ["customer_df", "branch_df", "transaction_df", "df_with_cumulative_sum",
                       "loyal_customers", "credit_risk_customers", "inactive_customers",
                       "recent_transactions", "new_user_customers", "high_value_customers"]

# Filter out the excluded DataFrames
dataframes_to_save = [(df, name) for name, df in dataframes.items() if name not in excluded_dataframes]

# Save each remaining DataFrame to a table
for df, name in dataframes_to_save:
    table_name = f"hive_metastore.gb_gold_schema.{name.replace('_df', '_table')}"  # Replace _df with _table in the name
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Saved DataFrame {name} to {table_name}")