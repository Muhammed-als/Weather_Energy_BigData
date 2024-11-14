import pygrib
#for parsing url
from urllib.parse import urlparse
import os

KELVIN = 273.15

url = 'https://dmigw.govcloud.dk/v1/forecastdata/download/HARMONIE_DINI_SF_2024-11-06T060000Z_2024-11-06T060000Z.grib?api-key=a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da'

#download file
#response = requests.get(url, verify=False)
#save file to.grib
#with open(filename, "wb") as file:
#  file.write(response.content)

#parse url for filename
parsed_url = urlparse(url)
filename = os.path.basename(parsed_url.path)

grbs = pygrib.open(filename)
temp0 = grbs.select(name='Temperature', level=0)[0]
temp0values = temp0.values
lats, lons = temp0.latlons()

for temp in grbs.select(name='Temperature'):
    print(temp.values[20][40] - KELVIN)

"""
ds_grib = xr.open_dataset(filename, engine="cfgrib")
#open downloaded files with xarray
ds = xr.open_mfdataset(
                      filename,
                      #compat='override',
                      combine = 'nested',
                      concat_dim ='valid_time',
                      engine="cfgrib")


print(ds_grib)
"""