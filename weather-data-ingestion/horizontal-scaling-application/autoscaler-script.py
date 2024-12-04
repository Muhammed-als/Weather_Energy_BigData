from kubernetes import client, config
import time
from src.calculate_topic_lag import calculate_consumer_group_lag
from src.utils import log, logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Load the Kubernetes configuration
CONFIG_PATH = 'weather-data-ingestion/atoscaler-application/group-07-kubeconfig'
config.load_kube_config(config_file=CONFIG_PATH)

# API clients
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()

# Constants
NAMESPACE = "group-07"
DEPLOYMENT_NAME = "example-deployment"
METRIC_THRESHOLD = 1  # if we have atleast 1 lag in consumer group we should scale
MIN_REPLICAS = 1
MAX_REPLICAS = 30

def get_custom_metric():
    topic = "FORECAST_DOWNLOAD_URLS"
    consumer_group = "FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP"

    total_lag, total_lag_count, lag_per_partition = calculate_consumer_group_lag(topic, consumer_group)
    log(f"Total Lag: {total_lag} across {total_lag_count} partition(s)")
    log(f"Lag Details per Partition: {lag_per_partition}")

    return total_lag, total_lag_count, lag_per_partition  # Mock value

def scale_deployment(replicas):
    deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
    deployment.spec.replicas = replicas
    apps_v1.patch_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE, deployment)
    log(f"Scaled {DEPLOYMENT_NAME} to {replicas} replicas.")


def checkMetrics(isStarted):
    metric_value, metric_count, metric_data = get_custom_metric()
    log(f"Current Metric Value: {metric_value}")

    # Read current number of replicas
    deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
    current_replicas = deployment.spec.replicas

    # Scaling logic
    if metric_value >= METRIC_THRESHOLD and current_replicas < MAX_REPLICAS and not isStarted:
        scale_deployment(MAX_REPLICAS)
        return True
    elif metric_count < current_replicas and current_replicas > MIN_REPLICAS:
        scale_deployment(current_replicas - 1)
        return True
    return False


def checkMetricsJob():
    crunchStarted = False
    while True:
        # Wait before checking metrics again
        crunchStarted = checkMetrics(crunchStarted)
        if not crunchStarted:
            break
        time.sleep(10)

def scheduleIntervalJob(scheduler):
    scheduler.add_job(checkMetricsJob, trigger=IntervalTrigger(minutes=2), id='metrics_job', replace_existing=True, max_instances=10)

scheduler = BlockingScheduler()
scheduleIntervalJob(scheduler)

checkMetrics()

scheduler.start()