import logging
from pprint import pprint
import requests
import schedule
import time
from dmi_open_data import DMIOpenDataClient
from tenacity import RetryError
from kafka_clients import KafkaProducer
from utils import weather_stations
from utils import log
api_key = '44509e99-cd08-4d5d-80f7-637beae711f1'
client = DMIOpenDataClient(api_key)
KAFKA_TOPIC_LIVE = 'DMI_METOBS'
KAFKA_TOPIC_AVERAGE = 'DMI_METOBS_AVERAGE'
AVRO_SCHEMA_LIVE = {
    "type": "record",
    "name": "MetObsData",
    "fields": [
        {"name": "stationId", "type": "string"},
        {"name": "municipality", "type": "string"},
        {"name": "energyGrid", "type": "string"},
        {"name": "coordinates", "type": {"type": "array", "items": "double"}},
        {"name": "properties", "type": {
            "type": "record",
            "name": "Properties",
            "fields": [
                {"name": "created", "type": "string"},
                {"name": "observed", "type": "string"},
            ]
        }},
        {"name": "values", "type": {
            "type": "record",
            "name": "Values",
            "fields": [
                {"name": "temp_dry", "type": "double"},
                {"name": "cloud_cover", "type": "double"},
                {"name": "humidity", "type": "double"},
                {"name": "wind_dir", "type": "double"},
                {"name": "wind_speed", "type": "double"}
            ]
        }}
    ]
}
AVRO_SCHEMA_AVERAGE = {
    "type": "record",
    "name": "MetObsValuesAverage",
    "fields": [
        {"name": "energyGrid", "type": "string"},
        {"name": "observed", "type": "string"},
        {"name": "values", "type": {
            "type": "record",
            "name": "Values",
            "fields": [
                {"name": "temp_dry", "type": "double"},
                {"name": "cloud_cover", "type": "double"},
                {"name": "humidity", "type": "double"},
                {"name": "wind_dir", "type": "double"},
                {"name": "wind_speed", "type": "double"}
            ]
        }}
    ]
}
def fetch_data():
    # Extract parameter IDs from AVRO schema dynamically
    parameter_ids = [
        field["name"] for field in AVRO_SCHEMA_LIVE["fields"][5]["type"]["fields"]
    ]
    stationIds = weather_stations.keys()
    # Append each parameter as a separate `parameterId`
    all_observations = []
    for stationId in stationIds:
        for param in parameter_ids:
            url = (
            "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
            "?period=latest"
            f"&api-key={api_key}"
            f"&parameterId={param}"
            f"&stationId={stationId}"
            )           
            try:
                # Make the GET request
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                all_observations.extend(data["features"])
            except requests.exceptions.HTTPError as http_err:
                log(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                log(f"Request error occurred: {req_err}")
    produceLiveData(all_observations)
    produceDataAverage(all_observations)
def produceDataAverage(observations):            
        producer = KafkaProducer(topic=KAFKA_TOPIC_AVERAGE, avro_schema=AVRO_SCHEMA_AVERAGE)
        all_records = []
        all_parameters_values = []
        for observation in observations:
            station_id = observation["properties"].get("stationId") 
            observed = observation["properties"].get("observed")
            parameter_id = observation["properties"].get("parameterId")
            value = observation["properties"].get("value")
            values = {parameter_id: value}
            for record in all_records:
                if record['stationId'] == station_id:
                    # Update the values in the existing record
                    record['values'].update(values)
                    break
            else:
                # Create a new record if `stationId` is not in `all_records`
                record = {
                    "stationId": station_id,
                    "values": values,
                }
                all_records.append(record)
                all_parameters_values.append({
                    "stationId": station_id,
                    "observed": observed,
                    "energyGrid" : weather_stations[station_id]['Egrid'],
                    "values": values
                })
        sums_DK1 = {}
        sums_DK2 = {}
        counts = {}
        record1 = {}
        record2 = {}
        for item in all_parameters_values:
            #station_id = item.get("stationId") 
            energyGrid = item.get('energyGrid',"")
            if  energyGrid == 'DK1':
                values = item.get('values', {})
                observed = item.get('observed',{})
                #record1["stationId"] = station_id  
                record1["energyGrid"] = energyGrid  
                record1["observed"] = observed  
                record1["values"] = {}
                for key, value in values.items():
                    if isinstance(value, (int, float)):  # Ensure the value is numeric
                        if key not in sums_DK1:
                            sums_DK1[key] = 0
                            counts[key] = 0
                        sums_DK1[key] += value
                        counts[key] += 1
            else:
                values = item.get('values', {})
                observed = item.get('observed',{})
                #record2["stationId"] = station_id  
                record2["energyGrid"] = energyGrid  
                record2["observed"] = observed  
                record2["values"] = {}
                for key, value in values.items():
                    if isinstance(value, (int, float)):  # Ensure the value is numeric
                        if key not in sums_DK2:
                            sums_DK2[key] = 0
                            counts[key] = 0
                        sums_DK2[key] += value
                        counts[key] += 1
        averages_DK1 = {key: sums_DK1[key] / counts[key] for key in sums_DK1}
        averages_DK2 = {key: sums_DK2[key] / counts[key] for key in sums_DK2}
        record1["values"].update(averages_DK1)
        record2["values"].update(averages_DK2)
        produce_key1 = f'DK1_{record1.get("observed")}'
        produce_key2 = f'DK2_{record2.get("observed")}'
        isFirstMessageProduced = producer.produce_message(produce_key1, record1)
        isSecondMessageProduced = producer.produce_message(produce_key2, record2)
        if isFirstMessageProduced and isSecondMessageProduced:
            log(f"Messages are successfully sent.")
        else:
            log(f"Failed to send messages")
        log(record1)
        log(record2) 
def produceLiveData(observations):            
        producer = KafkaProducer(topic=KAFKA_TOPIC_LIVE, avro_schema=AVRO_SCHEMA_LIVE)
        all_records = []
        for observation in observations:
            station_id = observation["properties"].get("stationId")
            properties = {
                "created": observation["properties"].get("created"),
                "observed": observation["properties"].get("observed"),  
            }
            parameter_id = observation["properties"].get("parameterId")
            value = observation["properties"].get("value")
            values = {parameter_id: value}
            for record in all_records:
                if record['stationId'] == station_id:
                    # Update the values in the existing record
                    record['values'].update(values)
                    break
            else:
                # Create a new record if `stationId` is not in `all_records`
                record = {
                    "stationId": station_id,
                    "properties": properties,
                    "municipality" : weather_stations[station_id]['name'],
                    "energyGrid" : weather_stations[station_id]['Egrid'],
                    "coordinates": {
                        weather_stations[station_id]['coordinates'][0],
                        weather_stations[station_id]['coordinates'][1]
                    },
                    "values": values,
                }
                all_records.append(record)
            produce_key = f'{station_id}_{record['properties'].get("observed")}'
            isMessageProduced = producer.produce_message(produce_key, record)
            if isMessageProduced:
                log(f"Message for station {station_id} successfully sent.")
            else:
                log(f"Failed to send message for station {station_id}.")
        log(all_records)
# Schedule the function to run every 10 minutes
schedule.every(10).minutes.do(fetch_data)

# Run the scheduler in a loop
#fetch_data()
while True:
    schedule.run_pending()
    time.sleep(1)


