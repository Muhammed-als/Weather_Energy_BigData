import requests
import schedule
import time
from dmi_open_data import DMIOpenDataClient
from tenacity import RetryError
from kafka_clients import KafkaProducer
from utils import weather_stations
api_key = '44509e99-cd08-4d5d-80f7-637beae711f1'
KAFKA_TOPIC: str = 'DMI_METOBS'
SCHEMA_REGISTRY = 'http://kafka-schema-registry:8081'
KAFKA_SERVER = 'kafka:9092'
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
            print(param)
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
                print("Live observations for all parameters:", all_observations)
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"Request error occurred: {req_err}")
    print(f"Total observations collected: {len(all_observations)}")
    produceData(all_observations, parameter_ids)
def produceData(observations, parameter_ids):            
        producer = KafkaProducer(kafka_server=KAFKA_SERVER, schema_registry=SCHEMA_REGISTRY, topic=KAFKA_TOPIC, avro_schema=AVRO_SCHEMA)
        coordinate = {}
        all_records = []
        for observation in observations:
            station_id = observation["properties"].get("stationId")
            coordinates = observation["geometry"].get("coordinates")
            coordinate[station_id] = coordinates
            properties = {
                "created": observation["properties"].get("created"),
                "datetime": observation["properties"].get("observed"),  
            }
            if(observation["properties"].get("parameterId") in parameter_ids):
                values = {
                    observation["properties"].get("parameterId"): observation["properties"].get("value")
            }
            # Construct the record
            record = {
                "stationId": station_id,
                "properties": properties,
                "values": values,
            }
            all_records.append(record)
        for record in all_records:
            station_id = record["stationId"]
            if station_id in weather_stations:
                record["municipality"] = weather_stations[station_id]['name']
                record["energyGrid"] = weather_stations[station_id]['Egrid']
                coordinates = [               
                weather_stations[station_id]['coordinates'][0],
                weather_stations[station_id]['coordinates'][1],
                ]
                record["coordinates"] = coordinates
            else:
                print(f"Station {station_id} not found in closest locations.")
        print("All records:", all_records) 
        """ isMessageProduced = producer.produce_message(key, record)
        if isMessageProduced:
            print(f"Message for station {station_id} successfully sent.")
        else:
            print(f"Failed to send message for station {station_id}.") """
# Schedule the function to run every 10 minutes
schedule.every(10).minutes.do(fetch_data)

# Run the scheduler in a loop
#fetch_data()
while True:
    schedule.run_pending()
    time.sleep(1)


