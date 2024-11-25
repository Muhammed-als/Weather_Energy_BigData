import pygrib
#for parsing url
from urllib.parse import urlparse
import os
import requests

from src.kafka_clients import KafkaConsumer, KafkaProducer
from src.utils import *
import numpy as np

KELVIN = 273.15

AVRO_SCHEMA = {
    "type": "record",
    "name": "MetObsData",
    "fields": [
    {"name": "municipality", "type": "string"},
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

DMI_FORECAST_DATA_LOG_TOPIC = 'DMI_FORECAST_DATA_LOG'
log_producer = KafkaProducer(DMI_FORECAST_DATA_LOG_TOPIC)

FORECAST_URL_TOPIC = 'FORECAST_DOWNLOAD_URLS'
offset = 'earliest'
groupId = 'FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP'

consumer = KafkaConsumer(FORECAST_URL_TOPIC, offset, groupId, use_avro=True, avro_schema=AVRO_SCHEMA, enable_auto_commit=False)
msg, filename, record = consumer.consume_message()

if record in None:
    log("No messages consumed. Exiting. . .")
    exit()

url = record['url']
#url = 'https://dmigw.govcloud.dk/v1/forecastdata/download/HARMONIE_DINI_SF_2024-11-06T060000Z_2024-11-06T060000Z.grib?api-key=a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da'

log(f'Message consumed: "{filename}" "{url}"')
log_producer.produce_message(f'Message consumed: "{filename}" "{url}"')

#parse url for filename
#parsed_url = urlparse(url)
#filename = os.path.basename(parsed_url.path)

if not os.path.exists(filename):
  # Download file
  response = requests.get(url, verify=False)
  # Save file to.grib
  with open(filename, "wb") as file:
    file.write(response.content)

PARAM_NAMES = {
  "High cloud cover",
  "Medium cloud cover",
  "Low cloud cover",
  "2 metre temperature",
  "2 metre specific humidity",
  "10 metre wind direction",
  "10 metre wind speed"
}

grbs = pygrib.open(filename)
_, lats, lons = grbs[1].data(lon1=7, lon2=16, lat1=54, lat2=58) # Reenforcing bbox of denmark since DMI returns way bigger bbox
latlons = np.stack((lats.flatten(), lons.flatten()), axis=-1)

# Get different parameter layers of gribfile
grib_param_list = {}
for param in PARAM_NAMES:
  grib_param_list[param] = grbs.select(name=param)[0]

DMI_FORECAST_DATA_TOPIC = 'DMI_FORECAST_DATA'

producer = KafkaProducer(topic=DMI_FORECAST_DATA_TOPIC, avro_schema=AVRO_SCHEMA)

for key, value in weather_stations:
  lat, lon = find_closest_geolocations_to_stations_from_grib(value['coordinates'], latlons)
  cloud_coverH = grib_param_list["High cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0]
  cloud_coverM = grib_param_list["Medium cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0]
  cloud_coverL = grib_param_list["Low cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0]
  cloud_cover = cloud_coverL + cloud_coverM * (1 - cloud_coverL) + cloud_coverH * (1 - max(cloud_coverL, cloud_coverM))
  message = {
    "stationId": key,
    "coordinates": (lat, lon),
    "properties": {
      record['properties']
    },
    "values": {
      "temp_dry": grib_param_list["2 metre temperature"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0],
      "cloud_cover": cloud_cover,
      "humidity": grib_param_list["2 metre temperature"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0],
      "wind_dir": grib_param_list["2 metre temperature"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0],
      "wind_speed": grib_param_list["2 metre temperature"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0][0]
    }
  }

  produce_key = f'{record['properties']['modelrun']}_{record['properties']['datetime']}'
  producer.produce_message(key=produce_key, record=message)

log_producer.produce_message(f"Model Run: {record['properties']['modelrun']}", f"Successfully produced {len(weather_stations)} messages")


#temp0 = grbs.select(name='Temperature', level=0)[0]
#temp0values = temp0.values
#lats, lons = temp0.latlons()

#for temp in grbs.select(name='Temperature'):
#    log(temp.values[20][40] - KELVIN)

"""
ds_grib = xr.open_dataset(filename, engine="cfgrib")
#open downloaded files with xarray
ds = xr.open_mfdataset(
                      filename,
                      #compat='override',
                      combine = 'nested',
                      concat_dim ='valid_time',
                      engine="cfgrib")


log(ds_grib)
"""