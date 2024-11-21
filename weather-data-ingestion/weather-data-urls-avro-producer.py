from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.dmi_forecast_client import DMIForecastClient
from src.kafka_clients import KafkaProducer, KafkaConsumer
from src.utils import *

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


def queryDMIandPushToKafka(modelrun_date = None, produce_regardless = False) -> int:
    # Get the current datetime rounded down to latest hour divisible by 3
    modelrun_datetime = get_current_model_run() if modelrun_date is None else modelrun_date
    log(f"Querying modelrun with date: {modelrun_datetime}")

    # Create producer from class above
    producer = KafkaProducer(topic=TOPIC_URLS, avro_schema=AVRO_SCHEMA)
    dmiCli = DMIForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

    # GET dmi data data to be sent
    keys, records, urlCount = dmiCli.getForecastUrls()

    if urlCount == 0 or (urlCount != 61 and not produce_regardless):
        return urlCount
    
    partition = 0
    for key, record in zip(keys, records):
        if not producer.produce_message(key, record, partition % 30):
            return urlCount
        partition += 1
    return urlCount

def queryLatestCommittetModelRun():
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

def clearSchedulerJobs(scheduler):
    scheduler.remove_all_jobs()
    scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=5), id='printstatus_job', replace_existing=True, max_instances=10)
    return



# Current model_run to be queried by intervalJob() and set in cronJob()
interval_model_run = ''

def cronJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    model_run = queryLatestCommittetModelRun()
    current_model_run = get_current_model_run()
    log(f"Latest committet model run:  {model_run}")
    log(f"Current model run is:        {current_model_run}")

    # If the latest model run matches the current model run then we already have the latest available. Otherwise get the next run
    if current_model_run == model_run:
        log("current model run == latest committet model run. returning from cronJob()")
        return
    model_run = get_next_model_run(model_run)
    log(f"Next model run to try toget: {model_run}")
    
     # While topic is not up to date, query the remaining model runs in order to catch up. push all collected urls regardless if there is exactly 61. If there are not 61 by now. We are not gonna get anymore from that run.
    while str_to_date(model_run) < str_to_date(current_model_run):
        urlCount = queryDMIandPushToKafka(model_run, produce_regardless=True)
        if urlCount == 0:
            producerLog.produce_message(f"Model Run: {model_run}", f"Failed to produced any URLS. Even though it was an old run")
        else:
            producerLog.produce_message(f"Model Run: {model_run}", f"Successfully produced {urlCount} messages from an old run")
        model_run = get_next_model_run(model_run)

    urlCount = queryDMIandPushToKafka(model_run)

    if urlCount == 61:
        producerLog.produce_message(f"Model Run: {model_run}", f"Successfully produced 61 messages")
        return
    else: # situation where the job didn't return true we need to retry every 5 min. 
        producerLog.produce_message(f"Model Run: {model_run}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Scheduling retry every 5 minutes")
        clearSchedulerJobs(scheduler)
        interval_model_run = model_run # update variable to hold the model_run to be queried in intervalJob()
        scheduler.add_job(intervalJob, trigger=IntervalTrigger(minutes=5), id='retry_job', replace_existing=True, max_instances=10)
        return

def intervalJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    current_model_run = get_current_model_run()

    if current_model_run != interval_model_run:
        log("current_model_run != interval_model_run - We must have skipped a model run, running cronJob to re-query old runs and catch up")
        clearSchedulerJobs(scheduler)
        scheduler.add_job(cronJob, CronTrigger(hour='*/3', minute=0), id='main_job', replace_existing=True, max_instances=10)
        cronJob()
        return
    
    urlCount = queryDMIandPushToKafka(interval_model_run)
    if urlCount == 61: # job finally returned true. reset to 3 hour intival cron job.
        producerLog.produce_message(f"Model Run: {interval_model_run}", f"Successfully produced 61 messages in retry")
        clearSchedulerJobs(scheduler)
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