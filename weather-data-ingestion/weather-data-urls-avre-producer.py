from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import requests
import json
from datetime import datetime, timedelta
from pprint import pprint

def on_msg_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} partition {msg.partition()} at offset {msg.offset()}')

class AvroKafkaProducer:
    def __init__(self, kafka_server, schema_registry, avro_schema, topic):
        # Initialize Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient(schema_registry)

        # Initialize AvroSerializer with the schema registry client and Avro schema
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            json.dumps(avro_schema)
        )
        self.string_serializer = StringSerializer('utf-8')
        self.producer = Producer(kafka_server)

        # Kafka topic to produce to
        self.topic = topic

    def produce_message(self, key, record):
        """
        Accepts a JSON data, validates it against the Avro schema, and produces it to Kafka.
        """
        try:
            # Produce the message to Kafka
            self.producer.produce(topic=self.topic, 
                                  key=self.string_serializer(key), 
                                  value=self.avro_serializer(record, SerializationContext(self.topic, MessageField.VALUE)),
                                  on_delivery=on_msg_delivery)
            self.producer.flush()  # Ensure all messages are sent

        except Exception as e:
            print(f"Failed to produce message: {e}")
            return

class DmiForecastClient:
    _model = 'harmonie_dini_sf'
    _model_run: str
    _api_key: str
    _bbox: list # rougly denmark

    def __init__(self, model_run, api_key, bbox):     
        self._model_run = model_run
        self._api_key = api_key
        self._bbox = bbox

    def getForecastUrls(self) -> list | list:
        url = f'https://dmigw.govcloud.dk/v1/forecastdata/collections/{self._model}/items?modelRun={self._model_run}&bbox={self._bbox}&api-key={self._api_key}'
        r = requests.get(url)

        if r.status_code != 200:
            print("Error request not 200")
            return {}, {} 

        data = r.json()
        if data['numberReturned'] != 61:
            print("Error not exactly 61 returned urls ")
            pprint(data)
            return {}, {} 

        return_list = []
        keys = []
        for feature in data['features']:
            list_element = {
                'url': feature['asset']['data']['href'],
                'bbox': feature['bbox'],
                'properties': feature['properties']
            }
            return_list.append(list_element)
            keys.append(feature['id'])

        return keys, return_list    



# Avro schema for the JSON structure
AVRO_SCHEMA = {
    "type": "record",
    "name": "ForecastURL",
    "fields": [
        {"name": "url", "type": "string"},
        {"name": "bbox", "type": {"type": "array", "items": "double"}},
        {"name": "properties", "type": {
            "type": "record",
            "name": "Properties",
            "fields": [
                {"name": "created", "type": "string"},
                {"name": "datetime", "type": "string"},
                {"name": "modelRun", "type": "string"}
            ]
        }}
    ]
}


# Get the current datetime rounded down to latest hour divisible by 3
current_datetime = datetime.now()
rounded_datetime = current_datetime - timedelta(hours=current_datetime.hour % 3 +3, # had to go back even further
                                                minutes=current_datetime.minute, 
                                                seconds=current_datetime.second, 
                                                microseconds=current_datetime.microsecond)
modelrun_datetime = rounded_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') #formattet to look like 'YYYY-MM_DDTHH:MM:SSZ'
print(modelrun_datetime)

# Kafka configs
kafkaServer = {'bootstrap.servers': 'kafka:9092'}
schemaRegistry = {'url': 'http://kafka-schema-registry:8081'} 
topic = 'FORECAST_DOWNLOAD_URLS'

# Create producer from class above
producer = AvroKafkaProducer(kafkaServer, schemaRegistry, AVRO_SCHEMA, topic)
dmiCli = DmiForecastClient(modelrun_datetime, 'a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', '7,54,16,58')

# GET dmi data data to be sent
keys, records = dmiCli.getForecastUrls()

if not keys:
    print("Returned lists from getForecastUrls were empty")
    exit(-1)

for key, record in zip(keys, records):
    producer.produce_message(key, record)
