import pygrib
#for parsing url
from urllib.parse import urlparse
import os
import requests

from src.kafka_clients import KafkaConsumer, KafkaProducer

KELVIN = 273.15

AVRO_SCHEMA = {
    "type": "record",
    "name": "MetObsData",
    "fields": [
		{"name": "stationId", "type": "string"},
		{"name": "coordinates", "type": {"type": "array", "items": "double"}},
		{"name": "properties", "type": {
			"type": "record",
			"name": "Properties",
			"fields": [
				{"name": "created", "type": "string"},
				{"name": "datetime", "type": "string"},
				{"name": "modelRun", "type": "string"}
			]
		}},		  
		{"name": "values", "type": {
			"type": "record",
			"name": "Values",
			"fields": [                                                 ######### Forecast equivalent ##########
                {"name": "temp_dry", "type": "double"},                 # 7
                {"name": "cloud_cover", "type": "double"},              # 1-3 
                #{"name": "cloud_height", "type": "double"},             # Not appclicable
                #{"name": "weather", "type": "string"},                  # Not appclicable
                {"name": "humidity", "type": "double"},                 # 45
                #{"name": "sun_last10min_glob", "type": "double"},       # Not appclicable
                #{"name": "wind_max", "type": "double"},                 # Not appclicable
                {"name": "wind_dir", "type": "double"},                 # 37
                {"name": "wind_speed", "type": "double"}                # 36
			]
			}
		}
    ]
}


FORECAST_URL_TOPIC = 'FORECAST_DOWNLOAD_URLS'
offset = 'earliest'
groupId = 'FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP'

consumer = KafkaConsumer(FORECAST_URL_TOPIC, offset, groupId, use_avro=True, avro_schema=AVRO_SCHEMA, enable_auto_commit=False)
msg, key, url = consumer.consume_message()

if url in None:
    print("No messages consumed. Exiting. . .")
    exit()

print(f'Message consumed: "{key}" "{url}"')

FORECAST_URL_LOG_TOPIC = 'FORECAST_DOWNLOAD_URLS_LOG'
log_producer = KafkaProducer(FORECAST_URL_LOG_TOPIC)

#url = 'https://dmigw.govcloud.dk/v1/forecastdata/download/HARMONIE_DINI_SF_2024-11-06T060000Z_2024-11-06T060000Z.grib?api-key=a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da'

#parse url for filename
parsed_url = urlparse(url)
filename = os.path.basename(parsed_url.path)

if not os.path.exists(key):
  # Download file
  response = requests.get(url, verify=False)
  # Save file to.grib
  with open(filename, "wb") as file:
    file.write(response.content)

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