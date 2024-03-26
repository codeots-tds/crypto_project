from dotenv import load_dotenv
import os
import requests as r
from datetime import datetime, timedelta
import time
import pandas as pd
import getpass
import json

load_dotenv()
curr_usr = os.getenv('USER') or os.getenv('USERNAME')
curr_usr = getpass.getuser()
cc_key = os.getenv('COINCAP_KEY')

cc_url = "https://api.coincap.io/v2/assets/bitcoin?"
cc_history_url = 'https://api.coincap.io/v2/assets/bitcoin/history'
headers={'Authorization': f'Bearer {cc_key}'} if cc_key else {}

current_time = datetime.now()
start_date = current_time - timedelta(days=5*365)
start_timestamp = int(start_date.timestamp()) * 1000
end_timestamp = int(current_time.timestamp()) * 1000
time_interval='d1'

def create_payload(time_interval, start_timestamp, end_timestamp):
    payload={
    # 'interval': 'd1', #daily interval,
    'interval': time_interval, #m1,
    'start': start_timestamp,
    'end': end_timestamp
    }
    return payload

def fetch_data(payload, api_url, headers):
        try:
            response = r.request("GET", api_url, headers=headers, params=payload)
            if response.status_code == 200:
                raw_data = response.json()
                call_timestamp = raw_data['timestamp']
            else:
                print('API request failed with status code:', response.status_code)
        except r.exceptions.RequestException as e:
            print('Network error:', e)
        except Exception as e:
            print('An error occurred:', e)
        return raw_data, call_timestamp

def adding_timestamp(data, timestamp):
        return {**data, 'call_timestamp': timestamp}

def add_call_timestamp_to_data(raw_data):
        raw_data = list(map(lambda entry: adding_timestamp(
            entry, raw_data['timestamp']), raw_data['data']))
        return raw_data

def savejson_hdfs(raw_data, call_timestamp):
        hdfs_url = f'/home/{curr_usr}/Desktop/portfolio_projects/crypto_project/app/data/datafiles/{call_timestamp}.json'
        directory = os.path.dirname(hdfs_url)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(hdfs_url, 'w') as f:
            json.dump(raw_data, f, indent=4)
            print('saved!')

if __name__=="__main__":
    payload = create_payload(time_interval='d1', 
                        start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    raw_data, call_timestamp = fetch_data(payload=payload, api_url = cc_history_url, headers=headers)
    raw_data = add_call_timestamp_to_data(raw_data = raw_data)
    savejson_hdfs(raw_data, call_timestamp)


#------------------------------------------------------------------------------------------------
# class Get_Data:
#     def __init__(self, **kwargs):
#         self.api_url = kwargs.get('url')
#         self.headers = kwargs.get('headers')
#         self.cc_key = kwargs.get('cc_key')
#         current_time = datetime.now()
#         start_date = current_time - timedelta(days=5*365)
#         self.start_timestamp = int(start_date.timestamp()) * 1000
#         self.end_timestamp = int(current_time.timestamp()) * 1000
#         self.payload = None
#         self.crypto_df = None
#         self.raw_data = None


#     def create_payload(self, time_interval):
#         self.payload={
#         # 'interval': 'd1', #daily interval,
#         'interval': 'm1', #m1,
#         'start': self.start_timestamp,
#         'end': self.end_timestamp
#         }

#     def fetch_data(self):
#         try:
#             response = r.request("GET", self.api_url, headers=self.headers, params=self.payload)
#             if response.status_code == 200:
#                 self.raw_data = response.json()
#                 self.call_timestamp = self.raw_data['timestamp']
#             else:
#                 print('API request failed with status code:', response.status_code)
#         except r.exceptions.RequestException as e:
#             print('Network error:', e)
#         except Exception as e:
#             print('An error occurred:', e)

#     @staticmethod
#     def adding_timestamp(data, timestamp):
#         return {**data, 'call_timestamp': timestamp}

#     def add_call_timestamp_to_data(self):
#         self.raw_data = list(map(lambda entry: Get_Data.adding_timestamp(
#             entry, self.raw_data['timestamp']), self.raw_data['data']))


#     def savejson_hdfs(self, call_timestamp):
#         hdfs_url = f'/home/{curr_usr}/Desktop/portfolio_projects/crypto_project/app/data/datafiles/{call_timestamp}.json'
#         directory = os.path.dirname(hdfs_url)
#         if not os.path.exists(directory):
#             os.makedirs(directory)
#         with open(hdfs_url, 'w') as f:
#             json.dump(self.raw_data, f, indent=4)
#             print('saved!')
#         pass

if __name__=="__main__":
    # btc_data_obj = Get_Data(
    #     url = cc_history_url,
    #     headers = headers,
    #     cc_key = cc_key,
    # )
    # btc_data_obj.create_payload(time_interval='d1')
    # btc_data_obj.fetch_data()
    # btc_data_obj.add_call_timestamp_to_data()
    # # print(btc_data_obj.raw_data)
    # btc_data_obj.savejson_hdfs(call_timestamp=btc_data_obj.call_timestamp)
    pass
