from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.dmi_forecast_client import DMIForecastClient
from src.kafka_clients import KafkaProducer

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
    rounded_datetime = current_datetime - timedelta(hours=(current_datetime.hour) % 3, # When accounting for beeing one hour ahead: hours=(current_datetime.hour-1) % 3 +1
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
    dmiCli = DMIForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

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