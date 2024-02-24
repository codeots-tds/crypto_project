import psycopg2

create_btc_price_tbl = (
"""
CREATE TABLE IF NOT EXISTS btc_price_tbl(
date DATE NOT NULL,
price_usd FLOAT,
time VARCHAR,
)
""")

create_btc_trade_table = ("""
CREATE TABLE IF NOT EXISTS btc_trade_tbl(



)
""")

create_table_queries = [create_btc_price_tbl

]