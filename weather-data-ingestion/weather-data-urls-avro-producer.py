from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.dmi_forecast_client import DMIForecastClient
from src.kafka_clients import KafkaProducer, KafkaConsumer
from src.utils import find_latest_date_string, get_next_model_run

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

def getCurrentModelrun() -> str:
    current_datetime = datetime.now()
    rounded_datetime = current_datetime - timedelta(hours=(current_datetime.hour-1) % 3 +1, # When accounting for beeing one hour ahead: hours=(current_datetime.hour-1) % 3 +1
                                                    minutes=current_datetime.minute, 
                                                    seconds=current_datetime.second, 
                                                    microseconds=current_datetime.microsecond)
    return rounded_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') #formattet to look like 'YYYY-MM-DDTHH:MM:SSZ'

def queryDMIandPushToKafka(modelrun_date = None) -> int:
    # Get the current datetime rounded down to latest hour divisible by 3
    modelrun_datetime = getCurrentModelrun() if modelrun_date is None else modelrun_date
    print(f"Querying modelrun with date: {modelrun_datetime}")

    # Kafka configs
    kafkaServer = 'kafka:9092'
    schemaRegistry = 'http://kafka-schema-registry:8081'
    topic = 'FORECAST_DOWNLOAD_URLS'

    # Create producer from class above
    producer = KafkaProducer(kafka_server=kafkaServer, schema_registry=schemaRegistry, topic=topic, avro_schema=AVRO_SCHEMA)
    dmiCli = DMIForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

    # GET dmi data data to be sent
    keys, records, urlCount = dmiCli.getForecastUrls()

    if not keys:
        print("Returned lists from getForecastUrls were empty")
        return urlCount
    
    partition = 0
    for key, record in zip(keys, records):
        if not producer.produce_message(key, record, partition % 30):
            return urlCount
        partition += 1
    return urlCount

# Initialize the scheduler
scheduler = BlockingScheduler()

def cronJob(model_run = None):
    # Kafka config for logging
    kafkaServer = 'kafka:9092'
    schemaRegistry = 'http://kafka-schema-registry:8081' 
    topic = 'FORECAST_DOWNLOAD_URLS_LOG'
    
    producerLog = KafkaProducer(kafka_server=kafkaServer, schema_registry=schemaRegistry, topic=topic)

    urlCount = queryDMIandPushToKafka(model_run)
    while model_run is not None and urlCount == 61:
        if getCurrentModelrun() == model_run:
            break
        producerLog.produce_message(f"ModelRun: {getCurrentModelrun()}", f"Successfully produced 61 messages from a previous run")
        model_run = get_next_model_run(model_run)
        urlCount = queryDMIandPushToKafka(model_run)

    if scheduler.get_job('main_job'):
        if urlCount == 61:
            producerLog.produce_message(f"ModelRun: {getCurrentModelrun()}", f"Successfully produced 61 messages")
            return
        else: # situation where the job didn't return true we need to retry every 5 min. 
            producerLog.produce_message(f"ModelRun: {getCurrentModelrun()}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Scheduling retry every 5 minutes")
            scheduler.remove_job('main_job')
            scheduler.add_job(cronJob, trigger=IntervalTrigger(minutes=5), id='retry_job', replace_existing=True, max_instances=10)
            return

    elif scheduler.get_job('retry_job'):
        if urlCount == 61: # job finally returned true. reset to 3 hour intival cron job.
            producerLog.produce_message(f"ModelRun: {getCurrentModelrun()}", f"Successfully produced 61 messages in retry")
            scheduler.remove_job('retry_job')
            scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0), id='main_job', replace_existing=True, max_instances=10)
            return
        else:
            producerLog.produce_message(f"ModelRun: {getCurrentModelrun()}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Retrying again in 5 minutes")
            return

def query_for_latest_model_run() -> bool:
    kafkaServer = 'kafka:9092'
    schemaRegistry = 'http://kafka-schema-registry:8081' 
    topic = 'FORECAST_DOWNLOAD_URLS'
    groupID = 'Forecast_URL_Query_Group'
    offset = 'latest'

    consumer = KafkaConsumer(kafka_server=kafkaServer, schema_registry=schemaRegistry, topic=topic, offset=offset, groupID=groupID, avro_schema=AVRO_SCHEMA)

    msgs = []
    keys = []
    values = []
    msg, key, value = consumer.consume_message()
    while msg is None:
        msg, key, value = consumer.consume_message()

    while msg is not None:
        msgs.append(msg)
        keys.append(key)
        values.append(value)
        msg, key, value = consumer.consume_message()

    consumer.close()

    return find_latest_date_string(keys)


def printJobSchedulerStatus():
    scheduler.print_jobs()

scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=5), id='printstatus_job', replace_existing=True, max_instances=10)
scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0, second=0), id='main_job', replace_existing=True, max_instances=10)

latest_model_run = query_for_latest_model_run()
if latest_model_run != getCurrentModelrun():
    cronJob(latest_model_run)

scheduler.start()