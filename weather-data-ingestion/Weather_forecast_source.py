#for HTTP requests
import requests
#for parsing json
import json
#for creating temporary directory
import tempfile
import os
#for working with multidimensional arrays
import xarray as xr
#for interactive plotting
#import hvplot.xarray
#choose relevant parameters such as model and time for model run and insert your apikey for forecast data from DMI
#for parsing url
from urllib.parse import urlparsepy
import cfgrib

model = 'harmonie_dini_sf'
model_run = '2024-10-31T06:00:00Z'
api_key = 'a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da'
bbox = '7,54,16,58' # rougly denmark

url = f'https://dmigw.govcloud.dk/v1/forecastdata/collections/{model}/items?modelRun={model_run}&bbox={bbox}&api-key={api_key}'
r = requests.get(url)
print(r.status_code)

data = r.json()
print(data['numberReturned'])

urls = [stac_item['asset']['data']['href'] for stac_item in data['features']]

#response = requests.get(urls[0], verify=False)
parsed_url = urlparse(urls[0])
filename = os.path.basename(parsed_url.path)
#print(filename)
#with open(filename, "wb") as file:
#  file.write(response.content)

ds_grib = xr.open_dataset(filename, engine="cfgrib")
#open downloaded files with xarray
ds = xr.open_mfdataset(
                      filename,
                      #compat='override',
                      combine = 'nested',
                      concat_dim ='valid_time',
                      engine="cfgrib")


print(ds_grib)