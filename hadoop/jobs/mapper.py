#!/usr/bin/env python3
"""
The mapper's job will be to read lines of JSON from standard input, parse them, 
add a call_timestamp field (for simplicity, 
we'll reuse the date field as the timestamp), 
and emit the result for further processing or reduction.
make sure script is executable: chmod +x mapper.py
"""
import sys
import json
from datetime import datetime

def preprocess_record(record_data, day_timestamp):
    # Add preprocessing steps here
    timestamp_s = day_timestamp/1000
    dt_obj = datetime.fromtimestamp(timestamp_s)
    date_str = dt_obj.strftime('%Y-%m-%d')
    datetime_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
    record_data['call_timestamp'] = [datetime_str, date_str]
    return record_data

#we're loading the data all at once here which is advised against but this is learning
# try:
#     input_data = sys.stdin.read()
#     records = json.loads(input_data)
#     for record in records:
#         print('ORIGINAL',json.dumps(record))
#         record = preprocess_record(record, record['call_timestamp'])
#         print('NEW',json.dumps(record))
# except json.JSONDecodeError as e:
#     print(f"Error parsing JSON: {e}", file=sys.stderr)

for line in sys.stdin:
    line = line.strip()  # Remove leading/trailing whitespace
    print(f"Processing line: {line}", file=sys.stderr)
    if not line:
        continue  # Skip empty lines
    try:
        record = json.loads(line)
        processed_record = preprocess_record(record, record['call_timestamp'])
        print(json.dumps(processed_record))
    except json.JSONDecodeError as e:
        # Optionally, log or handle the error
        print(f"Error parsing JSON: {e}", file=sys.stderr)