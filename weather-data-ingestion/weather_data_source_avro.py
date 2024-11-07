from confluent_kafka import avro, Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time
import requests

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather-data-topic'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# Weather API URL
API_URL = 'https://dmirestapi.example.com/metObs'  # Replace with actual endpoint

# Fetch weather data from DMI API
def fetch_weather_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Define Avro schema for the weather data
key_schema_str = '"string"'
value_schema_str = """
{
  "type": "record",
  "name": "WeatherForecastData",
  "fields": [
    {"name": "station_id", "type": "string"},
    {"name": "temp_dry", "type": ["null", "float"], "default": null},
    {"name": "humidity", "type": ["null", "float"], "default": null},
    {"name": "wind_speed", "type": ["null", "float"], "default": null},
    {"name": "wind_dir", "type": ["null", "float"], "default": null},
    {"name": "pressure", "type": ["null", "float"], "default": null},
    {"name": "precip_past1h", "type": ["null", "float"], "default": null},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# Initialize Schema Registry and AvroProducer
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

key_schema = avro.loads(key_schema_str)
value_schema = avro.loads(value_schema_str)

producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'schema.registry.url': SCHEMA_REGISTRY_URL
}

avro_producer = AvroProducer(
    producer_conf,
    default_key_schema=key_schema,
    default_value_schema=value_schema
)

# Produce data to Kafka topic
def produce_weather_data():
    weather_data = fetch_weather_data()

    if weather_data:
        for item in weather_data['data']:
            key = item['station_id']
            value = {
                "station_id": item['station_id'],
                "temp_dry": item.get('temp_dry'),
                "humidity": item.get('humidity'),
                "wind_speed": item.get('wind_speed'),
                "wind_dir": item.get('wind_dir'),
                "pressure": item.get('pressure'),
                "precip_past1h": item.get('precip_past1h'),
                "timestamp": int(time.time())
            }

            # Send the data to the Kafka topic
            avro_producer.produce(topic=TOPIC, key=key, value=value)
            print(f"Produced record: {key} -> {value}")

        # Flush producer to ensure all messages are delivered
        avro_producer.flush()

if __name__ == "__main__":
    while True:
        produce_weather_data()
        time.sleep(600)  # Poll the API every 10 minutes
