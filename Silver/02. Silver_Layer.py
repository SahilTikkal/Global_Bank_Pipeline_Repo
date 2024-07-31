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

# COMMAND ----------

# MAGIC %md
# MAGIC # Import tables
# MAGIC 1. customers table
# MAGIC 2. branch table
# MAGIC 3. transaction table

# COMMAND ----------

# DBTITLE 1,Importing Bronze tables
customer_df = spark.read.table("hive_metastore.gb_bronze_schema.customers_raw")
branch_df = spark.read.table("hive_metastore.gb_bronze_schema.branches_raw")
transaction_df = spark.read.table("hive_metastore.gb_bronze_schema.transactions_raw")

# COMMAND ----------

# DBTITLE 1,Creating stream for transaction table


# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Functions-
# MAGIC Function Descriptions:
# MAGIC - remove_duplicates(df, primary_keys): Removes duplicate rows based on primary keys.
# MAGIC - handle_missing_values(df, fill_values): Fills missing values with specified values.
# MAGIC - validate_data_types(df, schema): Ensures columns have the correct data types.
# MAGIC - trim_whitespaces(df): Removes leading and trailing whitespaces from string columns.
# MAGIC - remove_invalid_values(df, invalid_values): Removes rows with invalid or unrealistic values.
# MAGIC - enforce_uniqueness(df, unique_cols): Enforces uniqueness constraints on key columns.
# MAGIC - capitalize_string_data(df): Ensures that string data such as names are properly capitalized.
# MAGIC - flag_anomalies(df, anomaly_conditions): Flags rows with anomalies for further inspection.

# COMMAND ----------

# DBTITLE 1,FN: Cleaning Funtions
# 1. Remove duplicates
def remove_duplicates(df: DataFrame, primary_keys: list) -> DataFrame:
    """
    Remove duplicate rows based on primary keys.
    """
    return df.dropDuplicates(primary_keys)

# 2. Handling Missing Values
def handle_missing_values(df: DataFrame, fill_values: dict) -> DataFrame:
    """
    Fill missing values with specified values.
    """
    df1 = df.na.fill(fill_values)
    return df1.dropna()

# 3. Validate Data Type using schema
def validate_data_types(df: DataFrame, schema: dict) -> DataFrame:
    """
    Ensure columns have the correct data types.
    """
    for col_name, col_type in schema.items():
        df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df

# 4. Trimming White spaces
def trim_whitespaces(df: DataFrame) -> DataFrame:
    """
    Remove leading and trailing whitespaces from string columns.
    """
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(col(col_name)))
    return df

# 5. Remove Invalid Values
def remove_invalid_values(df: DataFrame, invalid_values: dict) -> DataFrame:
    """
    Remove rows with invalid or unrealistic values.
    """
    for col_name, value_range in invalid_values.items():
        df = df.filter((col(col_name) >= value_range[0]) & (col(col_name) <= value_range[1]))
    return df

# 6. Enforce Uniqueness on the primary key columns
def enforce_uniqueness(df: DataFrame, unique_cols: list) -> DataFrame:
    """
    Enforce uniqueness constraints on key columns.
    """
    return df.dropDuplicates(unique_cols)

# 7. Capitalize string data to make data homogenous
def capitalize_string_data(df: DataFrame) -> DataFrame:
    """
    Ensure that string data such as names are properly capitalized.
    """
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in string_cols:
        df = df.withColumn(col_name, upper(col(col_name)))
    return df
    
# 8. Flag anomalies using conditions 
def flag_anomalies(df: DataFrame, anomaly_conditions: dict) -> DataFrame:
    """
    Flag rows with anomalies for further inspection.
    """
    for col_name, condition in anomaly_conditions.items():
        df = df.withColumn("anomaly_flag", when(condition, True).otherwise(col("anomaly_flag")))
    return df

# COMMAND ----------

# DBTITLE 1,FN: Data Summery Functions
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
# MAGIC ## Customer table Cleaning

# COMMAND ----------

# DBTITLE 1,Customer table summery
# Print summary for the customer table
data_summary(customer_df)

# COMMAND ----------

# DBTITLE 1,Cleaning Customers dataframe
def clean_customers(customers_df: DataFrame) -> DataFrame:
    """
    Clean customers dataframe using the defined functions.
    """
    customers_df = remove_duplicates(customers_df, ["customer_id"])
    customers_df = handle_missing_values(customers_df, {"join_date": "2023-01-01"})
    customers_df = validate_data_types(customers_df, {"join_date": "date"})
    customers_df = trim_whitespaces(customers_df)
    customers_df = remove_invalid_values(customers_df, {"join_date": ["2015-01-01", "2023-12-31"]})
    customers_df = enforce_uniqueness(customers_df, ["customer_id"])
    customers_df = capitalize_string_data(customers_df)
    # Additional logic to flag anomalies can be added here
    
    return customers_df

cleaned_customers_df = clean_customers(customer_df)
cleaned_customers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branches Table

# COMMAND ----------

# DBTITLE 1,Branch Table summery
data_summary(branch_df)

# COMMAND ----------

# DBTITLE 1,Cleaning Branch Dataframe
def clean_branches(branches_df: DataFrame) -> DataFrame:
    """
    Clean branches dataframe using the defined functions.
    """
    branches_df = remove_duplicates(branches_df, ["branch_id"])
    branches_df = handle_missing_values(branches_df, {"location": "Unknown"})
    branches_df = trim_whitespaces(branches_df)
    branches_df = enforce_uniqueness(branches_df, ["branch_id"])
    branches_df = capitalize_string_data(branches_df)
    # Additional logic to flag anomalies can be added here
    
    return branches_df

cleaned_branch_df = clean_branches(branch_df)
cleaned_branch_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction table
# MAGIC

# COMMAND ----------

# DBTITLE 1,Transaction Table summery
data_summary(transaction_df)

# COMMAND ----------

# DBTITLE 1,FN: Cleaning duplicates in Transactions
def clean_duplicate_transactions(df: DataFrame, primary_keys: list, order_by_col: str) -> DataFrame:
    """
    Remove duplicate rows based on primary keys, keeping the last instance of duplicates.
    """
    # Define a window specification to partition by primary keys and order by order_by_col in descending order
    window_spec = Window.partitionBy(*primary_keys).orderBy(col(order_by_col).desc())

    # Add a row number column based on the window specification
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))

    # Filter rows to keep only the last instance (row_num == 1) and drop the row_num column
    df_deduplicated = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
    
    return df_deduplicated


# COMMAND ----------

# DBTITLE 1,Cleaning Transaction Dataframe
def clean_transactions(transactions_df: DataFrame) -> DataFrame:
    """
    Clean transactions dataframe using the defined functions.
    """
    transactions_df = clean_duplicate_transactions(transaction_df, ["transaction_id"], "timestamp")
    transactions_df = handle_missing_values(transactions_df, {"currency": "USD", "channel":"web"})
    transactions_df = validate_data_types(transactions_df, {"timestamp": "timestamp"})
    transactions_df = trim_whitespaces(transactions_df)
    transactions_df = remove_invalid_values(transactions_df, {"amount": [0, 100000]})
    transactions_df = enforce_uniqueness(transactions_df, ["transaction_id"])
    # Additional logic to flag anomalies can be added here
    
    return transactions_df

cleaned_transaction_df = clean_transactions(transaction_df)
display(cleaned_transaction_df)

# COMMAND ----------

# DBTITLE 1,Validating null count

null_counts = cleaned_transaction_df.select([sqlsum(col(c).isNull().cast("int")).alias(c) for c in transaction_df.columns])
display(null_counts)

# COMMAND ----------

# DBTITLE 1,validating cleaning on transaction df
data_summary(cleaned_transaction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Cleaning for all the data is done here
# MAGIC ----
# MAGIC ----
# MAGIC
# MAGIC ----
# MAGIC ----
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Tranformations and Data Enrichments

# COMMAND ----------

# MAGIC %md
# MAGIC #### General Transformations
# MAGIC 1. Standardize Column Values
# MAGIC      - Using this transformation we want to ensure our data is formatted same for whole data
# MAGIC 2. Convert Data Types
# MAGIC      - There are some columns whose data types are incorrect, using this functions we are typecasting our data to correct datatype
# MAGIC 3. Format Dates and Timestamps 
# MAGIC      - Formatting data and timestamps for better readability
# MAGIC 4. Normalize Currency
# MAGIC      - Normalising currency data to one demoninations
# MAGIC 5. Anonymize Sensitive Data
# MAGIC      - Replace sensitive data with anonymized or hashed values 
# MAGIC 6. Date and Time Manipulations
# MAGIC      - Extract parts of dates or perform calculations like time differences.
# MAGIC 7. Add Derived Columns (**data enrichment**)
# MAGIC      - Adding value to data using derived column 

# COMMAND ----------

# DBTITLE 1,FN: Format Dates and Timestamps
from pyspark.sql.functions import to_date, to_timestamp, date_format, col
from pyspark.sql import DataFrame

def format_dates(df: DataFrame, date_columns: list = [], timestamp_columns: list = []) -> DataFrame:
    """
    Formats specified columns in a DataFrame as dates or timestamps.
    """
    for col_name in date_columns:
        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, to_date(col(col_name)))
            except Exception as e:
                print(f"Error formatting column {col_name} to date: {e}")
        else:
            print(f"Column {col_name} does not exist in DataFrame.")

    for col_name in timestamp_columns:
        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, to_timestamp(col(col_name)))
                df = df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))
            except Exception as e:
                print(f"Error formatting column {col_name} to timestamp: {e}")
        else:
            print(f"Column {col_name} does not exist in DataFrame.")
    
    return df


# COMMAND ----------

# DBTITLE 1,FN: Extracting dates functions
def extract_date_parts(df: DataFrame, date_column: str, Year: bool = True, Month: bool = False, Day: bool = False) -> DataFrame:
    """
    Extracts parts of a date from a specified column in a DataFrame and adds them as new columns.
    """
    if date_column not in df.columns:
        raise ValueError(f"Column {date_column} does not exist in DataFrame.")
    
    try:
        if Year:
            df = df.withColumn("year", year(col(date_column)))
    except Exception as e:
        print(f"Error extracting year from column {date_column}: {e}")
    
    try:
        if Month:
            df = df.withColumn("month", month(col(date_column)))
    except Exception as e:
        print(f"Error extracting month from column {date_column}: {e}")
    
    try:
        if Day:
            df = df.withColumn("day", dayofmonth(col(date_column)))
    except Exception as e:
        print(f"Error extracting day from column {date_column}: {e}")
    
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers data

# COMMAND ----------

# DBTITLE 1,FN: Transformation Functions for Customers dataframe
def format_phone_numbers(df: DataFrame, phone_column: str) -> DataFrame:
    """
    Converts a phone number column to a numerical column by removing non-numeric characters.
    """
    # Remove non-numeric characters and cast to LongType
    df = df.withColumn(phone_column, regexp_replace(col(phone_column), "[^0-9]", "").cast(LongType()))
    
    return df

# Domain extraction
def domain_extract(df : DataFrame, col_name: str) -> DataFrame:
    """
    Extracts the domain from the email address column and adds it as a new column.
    """
    df = df.withColumn("domain", split(col(col_name), "@")[1])
    return df


# Postal code extraction
def extract_pincode(df: DataFrame, address_column: str, pincode_column: str) -> DataFrame:
    """
    Extracts the pincode (postal code) from the end of an address column and adds it as a new column.
    """
    # Extract the pincode (digits at the end of the address string)
    df = df.withColumn(pincode_column, regexp_extract(col(address_column), r'(\d+)$', 1).cast(IntegerType()))
    
    return df


# COMMAND ----------

# DBTITLE 1,Checking for data type missmatch
customer_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Formatting Dates and Timestamps
transformed_customer_df = format_dates(cleaned_customers_df, ["join_date"], ["last_update"])
transformed_customer_df.display()

# COMMAND ----------

# DBTITLE 1,Extracting Join date year
transformed_customer_df = extract_date_parts(transformed_customer_df, "join_date")
display(transformed_customer_df)

# COMMAND ----------

# DBTITLE 1,Transforming Phone number
transformed_customer_df = format_phone_numbers(transformed_customer_df, "phone")
display(transformed_customer_df)

# COMMAND ----------

# DBTITLE 1,Extraction Domain from Email
transformed_customer_df = domain_extract(transformed_customer_df, "email")
display(transformed_customer_df)

# COMMAND ----------

# DBTITLE 1,Extracting Pin code
transformed_customer_df = extract_pincode(transformed_customer_df, "address", "postal_code")
display(transformed_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch data

# COMMAND ----------

# DBTITLE 1,Checking for datatype mismatch
branch_df.printSchema()
display(branch_df)

# COMMAND ----------

# DBTITLE 1,FN: Getting Coordinate for locations
def get_coordinates(location: str) -> list:
    """
    Fetches coordinates for a given location using the Nominatim API.
    """
    time.sleep(2)
    # handling spaces in the location names
    if " " in location:
        location = location.replace(" ","_")

    encoded_location = quote(location)  # URL encode the location
    url = f'https://nominatim.openstreetmap.org/search?q={encoded_location}&format=json&addressdetails=1'
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            data = response.json()
            if data:
                lat = data[0]['lat']
                lng = data[0]['lon']
                return [float(lat), float(lng)]
            else:
                return [None, None]
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            # Exponential backoff
            time.sleep(2 ** attempt+1)  
    
    return [None, None]


# COMMAND ----------

# DBTITLE 1,Creating latitude and longitude columns
# Convert Spark DataFrame to pandas DataFrame
branch_pd_df = branch_df.toPandas()

# Apply the geocoding function to each location in the pandas DataFrame
branch_pd_df[['latitude', 'longitude']] = branch_pd_df['location'].apply(get_coordinates).apply(pd.Series)

# Convert the pandas DataFrame back to a Spark DataFrame
branch_transformed = spark.createDataFrame(branch_pd_df)

# Show the updated DataFrame
branch_transformed.display()


# COMMAND ----------

# DBTITLE 1,Rounding Latitude and Longitude
# Round the latitude and longitude columns to 2 decimal places
branch_transformed_rounded = branch_transformed.withColumn("latitude", round("latitude", 2))\
                                               .withColumn("longitude", round("longitude", 2))

display(branch_transformed_rounded)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions data

# COMMAND ----------

# DBTITLE 1,Capitalizing All Columns
transformed_transaction_df = capitalize_string_data(cleaned_transaction_df)
transformed_transaction_df.display()

# COMMAND ----------

# DBTITLE 1,Formatting Dates and Timestamps
transformed_transaction_df = format_dates(transformed_transaction_df, [], ["timestamp"])
transformed_transaction_df.display()

# COMMAND ----------

# DBTITLE 1,Extracting Year and Month
transformed_transaction_df = extract_date_parts(transformed_transaction_df, "timestamp", Month=True)
display(transformed_transaction_df)

# COMMAND ----------

# DBTITLE 1,FN: Conversion Rate function
def fetch_conversion_rates(base_currency='USD'):
    """
    Fetches conversion rates for a given base currency from ExchangeRate-API.
    """
    url = f'https://api.exchangerate-api.com/v4/latest/{base_currency}'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        rates = data.get('rates', {})
        return rates
    except requests.RequestException as e:
        print(f"Error fetching conversion rates: {e}")
        return {}


# COMMAND ----------

# DBTITLE 1,FN: Converting all amount to USD
def convert_currency_to_usd(df: DataFrame) -> DataFrame:
    """
    Converts currency amounts in a DataFrame to USD.
    """

    # conversion Rates
    conversion_rates = fetch_conversion_rates()
    
    # Broadcast the conversion rates dictionary
    broadcasted_conversion_rates = df.sql_ctx.sparkSession.sparkContext.broadcast(conversion_rates)

    # Define the UDF for currency conversion
    def convert_currency(amount, currency):
        if amount is None:
            return None  # Return None if amount is None
        rate = broadcasted_conversion_rates.value.get(currency, 1)  # Default to 1 if currency not found
        return amount / rate

    # Register the UDF
    convert_currency_udf = udf(convert_currency, DoubleType())

    # Apply the UDF to create the 'Amount_USD' column
    transformed_df = df.withColumn("Amount_USD", convert_currency_udf(col("amount"), col("currency")))

    return transformed_df


# COMMAND ----------

# DBTITLE 1,Creating Amount_USD column
transformed_transaction_df = convert_currency_to_usd(transformed_transaction_df)\
                            .withColumn("Amount_USD", round(col("Amount_USD"), 2))
transformed_transaction_df.display()

# COMMAND ----------

# DBTITLE 1,Validating transaction transformations
data_summary(transformed_transaction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Saving tables to Silver Layer

# COMMAND ----------

# DBTITLE 1,Saving Cleaned And Transformed Customers Table
transformed_customer_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_silver_schema.CAT_customers")

# COMMAND ----------

# DBTITLE 1,Saving Cleaned And Transformed Branches Table
branch_transformed_rounded.write.mode("overwrite").saveAsTable("hive_metastore.gb_silver_schema.CAT_Branches")

# COMMAND ----------

# DBTITLE 1,Saving Cleaned And Transformed Transformations Table
transformed_transaction_df.write.mode("overwrite").saveAsTable("hive_metastore.gb_silver_schema.CAT_Transformtions")