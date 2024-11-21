import requests
import schedule
import time
from dmi_open_data import DMIOpenDataClient
from tenacity import RetryError
from kafka_clients import KafkaProducer
api_key = '44509e99-cd08-4d5d-80f7-637beae711f1'
KAFKA_TOPIC: str = 'DMI_METOBS'
SCHEMA_REGISTRY = 'http://kafka-schema-registry:8081'
KAFKA_SERVER = 'kafka:9092'
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
# API key for DMI Open Data
client = DMIOpenDataClient(api_key)
# Define the data-fetching function
def fetch_data():
    try:
        stations = client.get_stations(limit=10)
    except RetryError as e:
        print(f"Failed to retrieve stations: {e}")
        return
    # Extract parameter IDs from AVRO schema dynamically
    parameter_ids = [
        field["name"] for field in AVRO_SCHEMA["fields"][3]["type"]["fields"]
    ]
    # Append each parameter as a separate `parameterId`
    for param in parameter_ids:
        url = (
        "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items"
        "?period=latest"
        f"&api-key={api_key}"
        f"&parameterId={param}"
        )           
        try:
            # Make the GET request
            response = requests.get(url)
            response.raise_for_status()
            observations = response.json()
            print("Live observations for all parameters:", observations)
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
        produceData(observations)
def produceData(observations):            
        producer = KafkaProducer(kafka_server=KAFKA_SERVER, schema_registry=SCHEMA_REGISTRY, topic=KAFKA_TOPIC, avro_schema=AVRO_SCHEMA)
        for observation in observations:
            try:
                # station_id = observation["properties"]["stationId"]
                coordinates = observation["geometry"]["coordinates"]
                properties = {
                "created": observation["properties"]["created"],
                "datetime": observation["properties"]["datetime"],
                "modelRun": observation["properties"]["modelRun"],
                }
                values = {
                "temp_dry": observation["properties"]["temp_dry"],
                "cloud_cover": observation["properties"]["cloud_cover"],
                "humidity": observation["properties"]["humidity"],
                "wind_dir": observation["properties"]["wind_dir"],
                "wind_speed": observation["properties"]["wind_speed"],
                }
                record = {
                "stationId": station_id,
                "coordinates": coordinates,
                "properties": properties,
                "values": values,
                }
                key = station_id
                isMessageProduced = producer.produce_message(key, record)
                if isMessageProduced:
                    print(f"Message for station {station_id} successfully sent.")
                else:
                    print(f"Failed to send message for station {station_id}.")
            except KeyError as e:
                print(f"Key error while processing observation: {e}")
            except Exception as ex:
                print(f"Unexpected error while processing observation: {ex}")
        
# Schedule the function to run every 10 minutes
schedule.every(10).minutes.do(fetch_data)

# Run the scheduler in a loop
while True:
    schedule.run_pending()
    time.sleep(1)