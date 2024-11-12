from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import requests
import json
from datetime import datetime, timedelta
from pprint import pprint
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

def on_msg_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} partition {msg.partition()} at offset {msg.offset()}')

class KafkaProducer:
    def __init__(self, kafka_server, schema_registry, topic, avro_schema = None):
        self.schema_registry_client = SchemaRegistryClient(schema_registry)

        # Initialize AvroSerializer with the schema registry client and Avro schema if schema is provided
        if avro_schema is not None:
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                json.dumps(avro_schema)
            )
        else:
            self.avro_serializer = None
        self.string_serializer = StringSerializer('utf-8')
        self.producer = Producer(kafka_server)
        self.topic = topic

    def produce_message(self, key, record) -> bool:
        try:
            # Produce the message to Kafka
            serializedValue = None
            if self.avro_serializer is not None:
                serializedValue = self.avro_serializer(record, SerializationContext(self.topic, MessageField.VALUE))
            else:
                serializedValue = self.string_serializer(record, SerializationContext(self.topic, MessageField.VALUE))


            self.producer.produce(topic=self.topic,
                                    key=self.string_serializer(key), 
                                    value=serializedValue,
                                    on_delivery=on_msg_delivery)
            self.producer.flush()  # Ensure all messages are sent
        except Exception as e:
            print(f"Failed to produce message: {e}")
            return False
        return True

class DmiForecastClient:
    _model = 'harmonie_dini_sf' # model Harmonie - Denmark, Iceland, Netherlands and Ireland - surface

    def __init__(self, model_run, api_key, bbox):     
        self._model_run = model_run
        self._api_key = api_key
        self._bbox = bbox

    def getForecastUrls(self) -> list | list | int:
        url = f'https://dmigw.govcloud.dk/v1/forecastdata/collections/{self._model}/items?modelRun={self._model_run}&bbox={self._bbox}&api-key={self._api_key}'
        r = requests.get(url)

        if r.status_code != 200:
            print(f"Error - request not 200, but insted {r.status_code}")
            return {}, {}, 0

        data = r.json()
        if data['numberReturned'] != 61:
            print("Error - Not exactly 61 returned urls ")
            print(f"numberReturned: {data['numberReturned']}")
            if data['numberReturned'] == 0:
                return {}, {}, data['numberReturned']
            #print("First data entry:")
            #pprint(data['features'][0])
            #if data['numberReturned'] > 1:
            #    print("Last entry:")
            #    pprint(data['features'][data['numberReturned']-1])
            return {}, {}, data['numberReturned']

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

        return keys, return_list, data['numberReturned']   



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

def getCurrentModelrunDatetimeRounded() -> str:
    current_datetime = datetime.now()
    rounded_datetime = current_datetime - timedelta(hours=(current_datetime.hour-1) % 3 +1, # accounting for beeing one hour ahead
                                                    minutes=current_datetime.minute, 
                                                    seconds=current_datetime.second, 
                                                    microseconds=current_datetime.microsecond)
    return rounded_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') #formattet to look like 'YYYY-MM-DDTHH:MM:SSZ'

def queryDMIandPushToKafka() -> int:
    # Get the current datetime rounded down to latest hour divisible by 3
    modelrun_datetime = getCurrentModelrunDatetimeRounded() 
    print(f"Querying modelrun with date: {modelrun_datetime}")

    # Kafka configs
    kafkaServer = {'bootstrap.servers': 'kafka:9092'}
    schemaRegistry = {'url': 'http://kafka-schema-registry:8081'} 
    topic = 'FORECAST_DOWNLOAD_URLS'

    # Create producer from class above
    producer = KafkaProducer(kafka_server=kafkaServer, schema_registry=schemaRegistry, topic=topic, avro_schema=AVRO_SCHEMA)
    dmiCli = DmiForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

    # GET dmi data data to be sent
    keys, records, urlCount = dmiCli.getForecastUrls()

    if not keys:
        print("Returned lists from getForecastUrls were empty")
        return urlCount
    
    for key, record in zip(keys, records):
        if not producer.produce_message(key, record):
            return urlCount
    
    return urlCount

# Initialize the scheduler
scheduler = BlockingScheduler()

def cronJob():
    # Kafka config for logging
    kafkaServer = {'bootstrap.servers': 'kafka:9092'}
    schemaRegistry = {'url': 'http://kafka-schema-registry:8081'} 
    topic = 'FORECAST_DOWNLOAD_URLS_LOG'
    
    producerLog = KafkaProducer(kafka_server=kafkaServer, schema_registry=schemaRegistry, topic=topic)

    urlCount = queryDMIandPushToKafka()
    if scheduler.get_job('main_job'):
        if urlCount == 61:
            producerLog.produce_message(f"ModelRun: {getCurrentModelrunDatetimeRounded()}", f"Successfully produced 61 messages to Kafkatopic FORECAST_DOWNLOAD_URLS")
            return
        else: # situation where the job didn't return true we need to retry every 5 min. 
            producerLog.produce_message(f"ModelRun: {getCurrentModelrunDatetimeRounded()}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Scheduling retry every 5 minutes")
            scheduler.remove_job('main_job')
            scheduler.add_job(cronJob, trigger=IntervalTrigger(minutes=5), id='retry_job', replace_existing=True)
            return

    elif scheduler.get_job('retry_job'):
        if urlCount == 61: # job finally returned true. reset to 3 hour intival cron job.
            producerLog.produce_message(f"ModelRun: {getCurrentModelrunDatetimeRounded()}", f"Successfully produced 61 messages to Kafkatopic FORECAST_DOWNLOAD_URLS in retry")
            scheduler.remove_job('retry_job')
            scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0), id='main_job', replace_existing=True)
            return
        else:
            producerLog.produce_message(f"ModelRun: {getCurrentModelrunDatetimeRounded()}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Retrying again in 5 minutes")
            return


def printJobSchedulerStatus():
    scheduler.print_jobs()

scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=1), id='printstatus_job', replace_existing=True)
scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0, second=0), id='main_job', replace_existing=True)

cronJob()

scheduler.start()