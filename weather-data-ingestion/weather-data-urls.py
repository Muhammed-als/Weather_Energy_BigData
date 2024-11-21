#for HTTP requests
import requests
#for parsing url
from urllib.parse import urlparse
from pprint import pprint

model = 'harmonie_dini_sf'
model_run = '2024-11-09T06:00:00Z'
api_key = 'a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da'
bbox = '7,54,16,58' # rougly denmark

url = f'https://dmigw.govcloud.dk/v1/forecastdata/collections/{model}/items?modelRun={model_run}&bbox={bbox}&api-key={api_key}'
r = requests.get(url)

if r.status_code != 200:
    # error
    pass 

data = r.json()
if data['numberReturned'] != 61:
    #error
    pass

urls = [stac_item['asset']['data']['href'] for stac_item in data['features']]
