import pandas as pd
from sqlalchemy import create_engine
import logging
import urllib
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # MySQL connection properties
        hostname = "127.0.0.1"  # Use IP address instead of 'localhost'
        port = "3306"
        database = "global_bank"  # Replace with your database name
        username = "root"  # Replace with your MySQL username
        password = "18111997@PS"  # Replace with your MySQL password

        # Encode special characters in the password
        password = urllib.parse.quote_plus(password)

        # Create the MySQL connection engine with explicit format
        connection_string = f"mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database}"
        engine = create_engine(connection_string)
        logger.info("MySQL connection engine created successfully.")

        # List of tables to read
        tables = ["branches", "customers"]

        # Read each table and store it locally as a Parquet file
        for table in tables:
            try:
                logger.info(f"Reading table: {table}")
                
                # Read data from MySQL into a DataFrame
                df = pd.read_sql_table(table, con=engine)
                logger.info(f"Data for table {table} read successfully.")

                # Save DataFrame to a Parquet file
                path = r"C:\Users\sahill\Desktop\Big data\CapstoneProject\Data"
                local_path = os.path.join(path, f"{table}.parquet")
                df.to_parquet(local_path, index=False)
                logger.info(f"Data for table {table} saved to {local_path}")

            except Exception as e:
                logger.error(f"An error occurred while processing table {table}: {e}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
