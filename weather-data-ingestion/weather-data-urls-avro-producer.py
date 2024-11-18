from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.dmi_forecast_client import DMIForecastClient
from src.kafka_clients import KafkaProducer, KafkaConsumer
from src.utils import find_latest_date_string, get_next_model_run

TOPIC_URLS = 'FORECAST_DOWNLOAD_URLS'
TOPIC_LOGGING = 'FORECAST_DOWNLOAD_URLS_LOG'

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

    # Create producer from class above
    producer = KafkaProducer(topic=TOPIC_URLS, avro_schema=AVRO_SCHEMA)
    dmiCli = DMIForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

    # GET dmi data data to be sent
    keys, records, urlCount = dmiCli.getForecastUrls()

    if not keys:
        return urlCount
    
    partition = 0
    for key, record in zip(keys, records):
        if not producer.produce_message(key, record, partition % 30):
            return urlCount
        partition += 1
    return urlCount

def query_latest_committet_model_run() -> bool:
    groupID = 'Forecast_URL_Query_Group'
    offset = 'earliest'

    consumer = KafkaConsumer(topic=TOPIC_URLS, offset=offset, groupID=groupID, avro_schema=AVRO_SCHEMA)

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

    latest_model_run = find_latest_date_string(keys)
    return latest_model_run

def clear_scheduler_jobs(scheduler):
    scheduler.remove_all_jobs()
    scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=5), id='printstatus_job', replace_existing=True, max_instances=10)
    return

def cronJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    model_run = query_latest_committet_model_run()
    print(f"Latest committet model run {model_run}")
    print(f"Current model run is       {getCurrentModelrun()}")

    # If the latest model run matches the current model run then we already have the latest available. Otherwise get the next run
    if getCurrentModelrun() == model_run:
        print("current model run == latest committet model run. returning from cronJob()")
        return
    model_run = get_next_model_run(model_run)
    print(f"Next model run found to be {model_run}. Trying to get that. . .")

    # While topic is not up to date, query the remaining model runs in order to catch up
    urlCount = queryDMIandPushToKafka(model_run)
    while urlCount == 61:
        if getCurrentModelrun() == model_run:
            break
        producerLog.produce_message(f"Model Run: {model_run}", f"Successfully produced 61 messages from a previous run")
        model_run = get_next_model_run(model_run)
        urlCount = queryDMIandPushToKafka(model_run)

    if urlCount == 61:
        producerLog.produce_message(f"Model Run: {model_run}", f"Successfully produced 61 messages")
        return
    else: # situation where the job didn't return true we need to retry every 5 min. 
        producerLog.produce_message(f"Model Run: {model_run}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Scheduling retry every 5 minutes")
        clear_scheduler_jobs(scheduler)
        interval_model_run = model_run # update variable to hold the model_run to be queried in intervalJob()
        scheduler.add_job(intervalJob, trigger=IntervalTrigger(minutes=5), id='retry_job', replace_existing=True, max_instances=10)
        return

# Current model_run to be queried by intervalJob()
interval_model_run = ''

def intervalJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    current_model_run = getCurrentModelrun()

    if current_model_run != interval_model_run:
        print("current_model_run != interval_model_run - We must have skipped a model run, running cronJob to re-query old runs and catch up")
        clear_scheduler_jobs(scheduler)
        scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0), id='main_job', replace_existing=True, max_instances=10)
        cronJob()
        return
    
    urlCount = queryDMIandPushToKafka(interval_model_run)
    if urlCount == 61: # job finally returned true. reset to 3 hour intival cron job.
        producerLog.produce_message(f"Model Run: {interval_model_run}", f"Successfully produced 61 messages in retry")
        clear_scheduler_jobs(scheduler)
        scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0), id='main_job', replace_existing=True, max_instances=10)
        return
    else:
        producerLog.produce_message(f"Model Run: {interval_model_run}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Retrying again in 5 minutes")
        return

def printJobSchedulerStatus():
    scheduler.print_jobs()

# Initialize the scheduler
scheduler = BlockingScheduler()

scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=5), id='printstatus_job', replace_existing=True, max_instances=10)
scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0, second=0), id='main_job', replace_existing=True, max_instances=10)

cronJob()

scheduler.start()