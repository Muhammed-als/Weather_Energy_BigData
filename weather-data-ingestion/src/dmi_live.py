import requests
import schedule
import time
from dmi_open_data import DMIOpenDataClient
from tenacity import RetryError
api_key = '44509e99-cd08-4d5d-80f7-637beae711f1'
KAFKA_TOPIC: str = 'DMI_METOBS'
KAFKA_BROKER: str = 'kafka_broker:9092'
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

# Schedule the function to run every 10 minutes
schedule.every(10).minutes.do(fetch_data)

# Run the scheduler in a loop
while True:
    schedule.run_pending()
    time.sleep(1)