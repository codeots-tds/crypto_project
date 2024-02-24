import subprocess
import json
import getpass
import os

curr_usr = os.getenv('USER') or os.getenv('USERNAME')
curr_usr = getpass.getuser()

def loadjson_hadoop_local(filename):
    data_path = f'/home/ra-terminal/Desktop/portfolio_projects/crypto_project/hadoop/data/input/{filename}'
    with open(data_path) as f:
        data = json.load(f)
        return data

def savejson_hadoop_local(raw_data, timestamp):
    timestamp = raw_data['timestamp']
    hdfs_url = f'/home/{curr_usr}/Desktop/portfolio_projects/crypto_project/hadoop/data/input/{timestamp}.json'
    with open(hdfs_url, 'w') as f:
        json.dump(raw_data, f, indent=4)
        print('saved!')



    
