from dotenv import load_dotenv
import os
import requests as r
from datetime import datetime, timedelta
import time
load_dotenv()

cc_key = os.getenv('COINCAP_KEY')

current_time = datetime.now()
start_date = current_time - timedelta(days=5*365)
start_timestamp = int(start_date.timestamp()) * 1000
end_date = current_time
end_timestamp = int(end_date.timestamp()) * 1000
cc_url = "https://api.coincap.io/v2/assets/bitcoin?"
cc_history_url= 'https://api.coincap.io/v2/assets/bitcoin/history'

payload={
    'interval': 'd1',
    'start': start_timestamp,
    'end': end_timestamp
}
headers={'Authorization': f'Bearer {cc_key}'} if cc_key else {}
response = r.request("GET", cc_history_url, headers=headers, params=payload)

print(response.status_code)
if response.status_code == 200:
    data = response.json()

print(data['data'][0])


