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
KAFKA_TOPIC: str = 'DMI_METOBS'
AVRO_SCHEMA = {
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
# API key for DMI Open Data
client = DMIOpenDataClient(api_key)
# Define the data-fetching function
def fetch_data():

    # Extract parameter IDs from AVRO schema dynamically
    
    parameter_ids = [
        field["name"] for field in AVRO_SCHEMA["fields"][5]["type"]["fields"]
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
    log(f"Total observations collected: {len(all_observations)}")
    produceData(all_observations)
def produceData(observations):            
        producer = KafkaProducer(topic=KAFKA_TOPIC, avro_schema=AVRO_SCHEMA)
        all_records = []
        print(len(observations))
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


