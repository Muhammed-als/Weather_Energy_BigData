from kafka import KafkaConsumer
import avro.schema
import avro.io
import io
import requests
import subprocess
import traceback

# Constants
KAFKA_FORECAST_TOPIC = "DMI_METOBS_AVERAGE"
KAFKA_BROKER = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://kafka-schema-registry:8081/subjects/DMI_METOBS_AVERAGE-value/versions/latest"

# Fetch Avro schema from the schema registry
try:
    print(f"Fetching schema from: {SCHEMA_REGISTRY_URL}")
    response = requests.get(SCHEMA_REGISTRY_URL)
    response.raise_for_status()
    schema_str = response.json()["schema"]
    avro_schema = avro.schema.parse(schema_str)
    print(f"Schema successfully fetched and parsed.")
except Exception as e:
    print(f"Error fetching schema: {e}")
    traceback.print_exc()
    exit(1)

# Initialize Kafka consumer
try:
    consumer = KafkaConsumer(
        KAFKA_FORECAST_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",  # Start from the latest offset
        enable_auto_commit=True
    )
    print(f"Connected to Kafka broker at {KAFKA_BROKER} and subscribed to topic {KAFKA_FORECAST_TOPIC}.")
except Exception as e:
    print(f"Error initializing Kafka consumer: {e}")
    traceback.print_exc()
    exit(1)

print("Waiting for new messages...")

try:
    for message in consumer:
        try:
            print(f"New Kafka message received: {message.value}")

            # Skip the first 5 bytes of the Avro header and decode the message
            bytes_reader = io.BytesIO(message.value[5:])
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(avro_schema)
            decoded_message = reader.read(decoder)
            print(f"Decoded message: {decoded_message}")

            # Extract relevant data
            weather_values = [
                decoded_message['values'].get('humidity'),
                decoded_message['values'].get('temp_dry'),
                decoded_message['values'].get('wind_speed'),
                decoded_message['values'].get('cloud_cover'),
            ]
            observed_time = decoded_message.get('observed')
            energy_grid = decoded_message.get('energyGrid')

            # Validate extracted data
            if None in weather_values or not observed_time or not energy_grid:
                print(f"Invalid data extracted: {weather_values}, {observed_time}, {energy_grid}")
                continue

            print(f"Weather values extracted: {weather_values}")
            print(f"Observed time: {observed_time}, Energy Grid: {energy_grid}")

            # Call the prediction script with the extracted data
            result = subprocess.run(
                [
                    "python3", "price_predict.py",
                    str(observed_time), str(energy_grid),
                    *map(str, weather_values)
                ],
                capture_output=True, text=True
            )

            print("Prediction script output:")
            print(result.stdout)
            print("Prediction script errors:")
            print(result.stderr)

        except Exception as decode_error:
            print(f"Failed to decode message: {decode_error}")
            traceback.print_exc()

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    consumer.close()
    print("Consumer closed.")
