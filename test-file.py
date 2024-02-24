import requests as r
import json

url = 'https://devhdidrupal01.health.dohmh.nycnet/hdi/epiquery/getTopicData/1'
url2 ='https://a816-health.nyc.gov/hdi/epiquery/getTopicData/1' #
res = r.get(url2)

print(res.status_code)
print(res.text)

"""
output
-2 directories
--one for topic jsons
--each subtopic with be each separate json file
"""