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

create_table_queries = [create_btc_price_tbl]