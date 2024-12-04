from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.dmi_forecast_client import DMIForecastClient
from src.kafka_clients import KafkaProducer, KafkaConsumer
from src.utils import *

import os

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
    log(f"Querying modelrun with date:  {modelrun_datetime}")

    # Create producer from class above
    producer = KafkaProducer(topic=TOPIC_URLS, avro_schema=AVRO_SCHEMA)
    dmiCli = DMIForecastClient(model_run=modelrun_datetime, api_key='a4a02c6a-ae8e-4ee6-97d4-0a99e656d3da', bbox='7,54,16,58')

    # GET dmi data data to be sent
    keys, records, urlCount = dmiCli.getForecastUrls()

    if urlCount == 0 or (urlCount != 61 and not produce_regardless):
        return urlCount
    
    for messageCount, keyValue in enumerate(zip(keys, records)):
        producer.produce_message(keyValue[0], keyValue[1]) #, messageCount % 30)
        producer.flush()
    return urlCount

def queryLatestCommittetModelRun(max_try_count = -1):
    groupID = 'Forecast_URL_Query_Group'
    offset = 'earliest'

    consumer = KafkaConsumer(topic=TOPIC_URLS, offset=offset, groupID=groupID, avro_schema=AVRO_SCHEMA)

    msgs = []
    keys = []
    values = []
    msg, key, value = consumer.consume_message()
    tries = 0
    while msg is None:
        tries += 1
        log(f"Consumed message was None. try {tries}", level=logging.ERROR)
        msg, key, value = consumer.consume_message()
        if tries >= max_try_count and max_try_count != -1:
            return None

    while msg is not None:
        msgs.append(msg)
        keys.append(key)
        values.append(value)
        msg, key, value = consumer.consume_message()

    consumer.close()

    latest_model_run = find_latest_date_string(keys)
    return latest_model_run

def clearSchedulerJobs(scheduler, status_interval_min = 30):
    scheduler.remove_all_jobs()
    scheduler.add_job(printJobSchedulerStatus, IntervalTrigger(minutes=status_interval_min), id='printstatus_job', replace_existing=True, max_instances=10)
    return

def scheduleCronJob(scheduler):
    scheduler.add_job(cronJob, CronTrigger(hour='1-23/3', minute=0, second=0), id='main_job', replace_existing=True, max_instances=10) # Old timer value */3, now fires every 3rd hour in the interval between 1-23 so: 1, 4, 7, 10, 13, 16, 19, 22

def scheduleIntervalJob(scheduler):
    scheduler.add_job(intervalJob, trigger=IntervalTrigger(minutes=5), id='retry_job', replace_existing=True, max_instances=10)

# Current model_run to be queried by intervalJob() and set in cronJob()
interval_model_run = ''

def cronJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    model_run = queryLatestCommittetModelRun(10)
    current_model_run = get_current_model_run()
    log(f"Current model run is:         {current_model_run}")
    if model_run is None:
        log(f"No messages found in topic")
        model_run = get_earliest_possible_model_run(current_model_run)
        log(f"Earliest possible model run:  {model_run}")
    else:
        log(f"Latest committet model run:   {model_run}")

    # If the latest model run matches the current model run then we already have the latest available. Otherwise get the next run
    if current_model_run == model_run:
        log("current model run == latest committet model run. returning from cronJob()")
        return
    model_run = get_next_model_run(model_run)
    log(f"Next model run to try to get: {model_run}")
    
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
        clearSchedulerJobs(scheduler, status_interval_min=5)
        interval_model_run = model_run # update variable to hold the model_run to be queried in intervalJob()
        scheduleIntervalJob(scheduler)
        return

def intervalJob():
    producerLog = KafkaProducer(topic=TOPIC_LOGGING)

    current_model_run = get_current_model_run()

    if current_model_run != interval_model_run:
        log("current_model_run != interval_model_run - We must have skipped a model run, running cronJob to re-query old runs and catch up")
        clearSchedulerJobs(scheduler)
        scheduleCronJob(scheduler)
        cronJob()
        return
    
    urlCount = queryDMIandPushToKafka(interval_model_run)
    if urlCount == 61: # job finally returned true. reset to 3 hour intival cron job.
        producerLog.produce_message(f"Model Run: {interval_model_run}", f"Successfully produced 61 messages in retry")
        clearSchedulerJobs(scheduler)
        scheduleCronJob(scheduler)
    else:
        producerLog.produce_message(f"Model Run: {interval_model_run}", f"Failed to produce URLS. Returned urlcount: {urlCount}. Retrying again in 5 minutes")
        return

def printJobSchedulerStatus():
    #scheduler.print_jobs()
    log("Job Scheduler Status - Printing registered jobs:")
    for job in scheduler.get_jobs():
        log(f"\t{job}")

log(f"Starting weather-forecast-url-producer with id: {os.getenv("HOSTNAME")}")

# Initialize the scheduler
scheduler = BlockingScheduler()

scheduleCronJob(scheduler)

cronJob()

scheduler.start()