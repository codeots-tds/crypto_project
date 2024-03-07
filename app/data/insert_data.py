
"""
CREATE TABLE IF NOT EXISTS btc_price_tbl(
    date DATE NOT NULL,
    price_usd FLOAT,
    time VARCHAR,
    call_timestamp VARCHAR
)
"""



create_data_queries = [insert_price_tbl]