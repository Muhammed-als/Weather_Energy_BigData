import pygrib
#for parsing url
from urllib.parse import urlparse
import os
import requests

from src.kafka_clients import KafkaConsumer, KafkaProducer
from src.utils import *
import numpy as np

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

KELVIN = 273.15

AVRO_SCHEMA = {
    "type": "record",
    "name": "ForecastDataAvgHARMONIE",
    "fields": [
        {"name": "energyGrid", "type": "string"},
        {"name": "observed", "type": "string"},
        {"name": "values", "type": {
            "type": "record",
            "name": "Values",
            "fields": [                                                 ######### Forecast equivalent ##########
                {"name": "temp_dry", "type": "double"},                 # 7
                {"name": "cloud_cover", "type": "double"},              # 1-3 
                {"name": "humidity", "type": "double"},                 # 45
                {"name": "wind_dir", "type": "double"},                 # 37
                {"name": "wind_speed", "type": "double"}                # 36
            ]
            }
        }
    ]
}

def download_and_process_forecast_data() -> bool:
    DMI_FORECAST_DATA_LOG_TOPIC = 'DMI_FORECAST_DATA_LOG'
    log_producer = KafkaProducer(DMI_FORECAST_DATA_LOG_TOPIC)

    FORECAST_URL_TOPIC = 'FORECAST_DOWNLOAD_URLS'
    offset = 'latest'
    groupId = 'FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP'
    clientId = f'consumer-{os.getenv("HOSTNAME")}'
    partitionAssignmetStrat = 'roundrobin'
    log(f'Initiating consumer for topic: {FORECAST_URL_TOPIC}, offset: {offset}, groupId: {groupId}, clientId: {clientId}, partition_assignmet_strat: {partitionAssignmetStrat}')
    consumer = KafkaConsumer(FORECAST_URL_TOPIC, offset, groupId, use_avro=True, enable_auto_commit=False, client_id=clientId, partition_assignmet_strat=partitionAssignmetStrat)

    # try to query for next message until 
    NoValidUrlFound = True
    while NoValidUrlFound:
        
        for i in range(3):
            log(f'Polling for next url (60s). . . try {i+1}')
            msg, filename, record = consumer.consume_message(60)
            if record is not None:
                break
        if record is None:
            return False

        url = record['url']

        log(f'Key: "{filename}" consumed from partition: {msg.partition()} value: "{url}"')
        log_producer.produce_message(f'{record['properties']['modelRun']}_{record['properties']['datetime']}', record=f'Key: "{filename}" consumed from partition: {msg.partition()} value: "{url}"')

        if not os.path.exists(filename):
            # Download file
            response = requests.get(url)
            if response.status_code != 200:
                log_producer.produce_message(f'{record['properties']['modelRun']}_{record['properties']['datetime']}', record=f'Url did not return 200, but instead {response.status_code}. Maybe it\'s too old')
                log(f'Url did not return 200, but instead {response.status_code}. Maybe it\'s too old. Committing message because it\'s obsolete', level=logging.ERROR)
                consumer.commit(msg=msg)
                continue  
            
            # Save file to.grib
            with open(filename, "wb") as file:
                file.write(response.content)

        #Found a url that did not return False
        NoValidUrlFound = False


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
    if len(grbs) == 0:
        log('Grib file was empty', level=logging.ERROR)
        return False

    _, lats, lons = grbs[1].data(lon1=7, lon2=16, lat1=54, lat2=58) # Reenforcing bbox of denmark since DMI returns way bigger bbox
    latlons = np.stack((lats.flatten(), lons.flatten()), axis=-1)

    # Get different parameter layers of gribfile
    grib_param_list = {}
    for param in PARAM_NAMES:
        grib_param_list[param] = grbs.select(name=param)[0]

    DMI_FORECAST_DATA_AVG_TOPIC = 'DMI_FORECAST_DATA_AVG'

    # Try to query for all messages already in topic to make sure we do not make dublicates 
    consumer_ckeck_dublicate = KafkaConsumer(DMI_FORECAST_DATA_AVG_TOPIC, 'earliest', 'DMI_FORECAST_AVG_PRODUCER_DUBLICATE_CHECK_GROUP', use_avro=True, enable_auto_commit=False)
    filenames_already_in_topic = []
    for i in range(10):
        _, filename_in_topic, _ = consumer_ckeck_dublicate.consume_message()
        if filename_in_topic is None:
            log(f"No messages consumed. when querying for messages already in {DMI_FORECAST_DATA_AVG_TOPIC} - try: {i}")
        else:
            break 
    while filename_in_topic is not None:
        filenames_already_in_topic.append(filename_in_topic)
        _, filename_in_topic, _ = consumer_ckeck_dublicate.consume_message()
    consumer_ckeck_dublicate.close()
    log(f"Consumed topic {DMI_FORECAST_DATA_AVG_TOPIC} and found {len(filenames_already_in_topic)} messages")


    producer = KafkaProducer(topic=DMI_FORECAST_DATA_AVG_TOPIC, avro_schema=AVRO_SCHEMA)

    missed_attempts = 0
    DK1 = {
        "Egrid": "DK1",
        "key": f'DK1_{record['properties']['modelRun']}_{record['properties']['datetime']}',
        "temp_dry": 0,
        "cloud_cover": 0,
        "humidity": 0,
        "wind_dir": 0,
        "wind_speed": 0,
        "count": 0
    }

    DK2 = {
        "Egrid": "DK2",
        "key": f'DK2_{record['properties']['modelRun']}_{record['properties']['datetime']}',
        "temp_dry": 0,
        "cloud_cover": 0,
        "humidity": 0,
        "wind_dir": 0,
        "wind_speed": 0,
        "count": 0
    }

    for key, values in weather_stations.items():
        # Skip key if already in topic
        if (weather_stations[key]["Egrid"] == "DK1"):
            tmp = DK1
        else:
            tmp = DK2
        tmp["count"] += 1
        
        #lat, lon = find_closest_geolocations_to_stations_from_grib(values['coordinates'], latlons)
        lat, lon = weather_stations_forecast_latlons[key]
        cloud_coverH = grib_param_list["High cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]
        cloud_coverM = grib_param_list["Medium cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]
        cloud_coverL = grib_param_list["Low cloud cover"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]
        cloud_cover = (cloud_coverL + 
                      cloud_coverM * (1 - cloud_coverL/100) + 
                      cloud_coverH * (1 - (cloud_coverL + cloud_coverM * (1 - cloud_coverL/100) )/100 ) )

        tmp["temp_dry"] += grib_param_list["2 metre temperature"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0] - KELVIN
        tmp["cloud_cover"] += cloud_cover
        tmp["humidity"] += grib_param_list["2 metre specific humidity"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]
        tmp["wind_dir"] += grib_param_list["10 metre wind direction"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]
        tmp["wind_speed"] += grib_param_list["10 metre wind speed"].data(lat1=lat, lat2=lat, lon1=lon, lon2=lon)[0][0]

        log(f'Working on {tmp['Egrid']}, key: {key} - generated values: temp_dry: {tmp["temp_dry"]}, cloud_cover: {tmp["cloud_cover"]}, humidity: {tmp["humidity"]}, wind_dir: {tmp["wind_dir"]}, wind_speed: {tmp["wind_speed"]}')

    
    for DKx in (DK1, DK2):
        if DKx["key"] in filenames_already_in_topic:
            log(f'Found key already in topic: {DKx["key"]}. Skipping this one')
            continue
        else:
            log(f'Generated average for {DKx['Egrid']}, count {DKx["count"]} - AVG values: temp_dry: {DKx["temp_dry"] / DKx["count"]}, cloud_cover: {DKx["cloud_cover"] / DKx["count"]}, humidity: {DKx["humidity"] / DKx["count"]}, wind_dir: {DKx["wind_dir"] / DKx["count"]}, wind_speed: {DKx["wind_speed"] / DKx["count"]}')


        message = {
            "energyGrid": DKx["Egrid"],
            "observed": record['properties']['datetime'],
            "values": {
                "temp_dry": DKx["temp_dry"] / DKx["count"],
                "cloud_cover": cloud_cover / DKx["count"],
                "humidity": DKx["humidity"] / DKx["count"],
                "wind_dir": DKx["wind_dir"] / DKx["count"],
                "wind_speed": DKx["wind_speed"] / DKx["count"]
            }
        }

        if not producer.produce_message(key=DKx["key"], record=message):
            continue

        log_string = f"Successfully produced {DKx["key"]} to {DMI_FORECAST_DATA_AVG_TOPIC}"
        log_producer.produce_message(DKx["key"], log_string)
        log(log_string)

    #wait for all messages to be delivered
    producer.flush()
    # commit to consumed msg declaring that the operation has exited usccessfully and should not be retried
    consumer.commit(msg=msg)
    consumer.close()

    #remove file as it is no longer needed
    os.remove(filename)

    log_producer.flush()

    return True


def downloadAndProcessDataJob():
    if scheduler.running:
        scheduler.pause()
    while download_and_process_forecast_data():
        time.sleep(10)
    if scheduler.running:
        scheduler.resume()

def clearAllJobs(scheduler):
    scheduler.remove_all_jobs()

def scheduleIntervalJob(scheduler):
    scheduler.add_job(downloadAndProcessDataJob, trigger=IntervalTrigger(minutes=2), id='metrics_job', replace_existing=True, max_instances=10)

log(f"Starting weather-forecast-data-producer with id: {os.getenv("HOSTNAME")}")

scheduler = BlockingScheduler()
scheduleIntervalJob(scheduler)

downloadAndProcessDataJob()

scheduler.start()