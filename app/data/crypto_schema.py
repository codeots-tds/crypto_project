import psycopg2

create_btc_price_tbl = (
"""
CREATE TABLE IF NOT EXISTS btc_price_tbl(
    date DATE NOT NULL,
    price_usd FLOAT,
    time VARCHAR,
    call_timestamp VARCHAR
)
""")

create_btc_agg_tbl = ("""
    CREATE TABLE IF NOT EXISTS btc_agg_tbl(
    date DATE NOT NULL,
    avg_price FLOAT,
    median_price FLOAT,
    count INT,
    std_dev FLOAT,
    variance FLOAT,
    mode FLOAT
""")


create_table_queries = [create_btc_price_tbl, create_btc_agg_tbl]