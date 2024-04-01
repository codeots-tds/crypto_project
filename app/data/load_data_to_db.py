import pandas as pd
import psycopg2
import os
import io
from dotenv import load_dotenv, find_dotenv
from time import sleep
import json

# dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
dotenv_path = '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/.env'
load_dotenv(dotenv_path=dotenv_path)
print("Resolved dotenv_path:", dotenv_path)

db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')

# print("DB_HOST:", db_host)
# print("DB_USER:", db_user)
# print("DB_PASS:", db_pass)

def create_conn(max_retries = 5):
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', '')
        )
            # print(f"Dotenv path: {dotenv_path}")
            # print("DB_HOST:", os.getenv('DB_HOST'))
            # print("DB_NAME:", os.getenv('DB_NAME'))
            # print("DB_USER:", os.getenv('DB_USER'))
            # print("DB_PASSWORD:", os.getenv('DB_PASSWORD'))
            # print('Connection successfull')
            return conn
        except psycopg2.OperationalError as e:
            print('Unable to connect to the database, Retrying')
            print(e)
            sleep(5)
            retries += 1
    return conn

def create_table(conn, cur, sql_st):
    try:
        cur.execute(sql_st)
        print(f"""Table {sql_st} was successfully created!""")
        conn.commit()
    except psycopg2.Error as e:
        print("Error: Issue creating table!")
        print(e)

def gen_placeholder_vals(col_count):
    return ', '.join(['%s'] * col_count)

def insert_data(conn, cur, tablename, filepath):
    try:
        with open(filepath, 'r') as file:
            for line in file:
                data = json.loads(line)
                # Dynamically extract values based on colnames
                # print(data.keys())
                print("Original data keys:", data.keys())
                data['price_usd'] = data.pop('priceUsd', None)
                print("Modified data keys:", data.keys())
                colnames = list(data.keys())
                col_num = len(colnames)
                placeholders = gen_placeholder_vals(col_num)
                colnames_str = ', '.join(colnames)  # Joins column names for SQL statement
                values = tuple(data.get(col, None) for col in colnames)
                # print(f"""
                #     INSERT INTO {tablename} ({colnames_str})
                #     VALUES ({placeholders})
                # """, values)
                cur.execute(f"""
                    INSERT INTO {tablename} ({colnames_str})
                    VALUES ({placeholders})
                """, values)
        conn.commit()
        print("Data insertion complete.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error inserting data: {error}")
        conn.rollback()  # Rollback on error

def process_directory(conn, cur, tablename, directory):
    for filename in os.listdir(directory):
        if filename.startswith('part-') and not filename.endswith('_SUCCESS'):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                print(f"Processing file: {filepath}")
                insert_data(conn, cur, tablename, filepath)

def delete_table_data(sql_st, tablename):
    query = f'DELETE FROM {tablename}'
    try:
        cur.execute(query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error: Couldn't delete data from {tablename} table")
        print(e)
    else:
        return "Data was loaded!"
    finally:
        conn.close()

conn = create_conn()
cur = conn.cursor()
# for table_query in create_table_queries:
#     create_table(conn, cur, table_query)
#     print('creating table...', table_query)

tablename = 'btc_price_tbl'
directory =  '/home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/data/temp_data'
process_directory(conn, cur, tablename, directory)

conn.commit()
cur.close()
conn.close()

if __name__ == '__main__':
    # conn = create_conn()
    # cur = conn.cursor()
    # cur.execute(drop_data_table)
    # for table_st in create_tables:
    #      create_table(conn = conn, cur = cur, sql_st = table_st)
    # insert_data(df = subway_station_df, sql_st = insert_subway_data_query, tablename='subway_station_table')

    pass