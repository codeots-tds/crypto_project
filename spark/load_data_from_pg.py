from pyspark.sql import SparkSession, SQLContext
import pyspark.pandas as ps
import psycopg2
import pandas as pd

import io
import os
# Set the environment variables for PySpark
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'
from dotenv import load_dotenv, find_dotenv
# dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
dotenv_path = '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/.env'
load_dotenv(dotenv_path=dotenv_path)
print("Resolved dotenv_path:", dotenv_path)

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_name = os.getenv('DB_NAME')
db_pass = os.getenv('DB_PASSWORD')
db_url = os.getenv("DB_URL")

conn_params ={
    "host": db_host,
    "database": db_name,
    "user": db_user,
    "password": db_pass
}

# Establish a connection to the database
conn = psycopg2.connect(**conn_params)

table_name = 'btc_price_tbl'
# Execute a query
sql_query = f"SELECT * FROM {table_name}"
btc_df = pd.read_sql_query(sql_query, conn)
conn.close()
spark = SparkSession.builder \
            .appName("Crypto_Project") \
            .master("local") \
            .config('spark.ui.port', '4040') \
            .getOrCreate()

sbtc_df = spark.createDataFrame(btc_df)
psbtc_df = sbtc_df.to_pandas_on_spark()

