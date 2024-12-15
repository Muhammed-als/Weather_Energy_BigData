from kubernetes import client, config
import time
from src.calculate_topic_lag import calculate_consumer_group_lag
from src.utils import log, logging
from src.kafka_clients import KafkaProducer

# Load the Kubernetes configuration
CONFIG_PATH = 'src/group-07-kubeconfig'
config.load_kube_config(config_file=CONFIG_PATH)

DMI_FORECAST_DATA_LOG_TOPIC = 'DMI_FORECAST_DATA_LOG'
log_producer = KafkaProducer(DMI_FORECAST_DATA_LOG_TOPIC)

# API clients
apps_v1 = client.AppsV1Api()

# Constants
NAMESPACE = "group-07"
KUBERNETES_OBJECT_TYPE = "deployment" #"statefulset" #"deployment"
DEPLOYMENT_NAME = "weather-forecast-data-avg-producer"
METRIC_THRESHOLD = 1  # if we have atleast 1 lag in consumer group we should scale
MIN_REPLICAS = 1
MAX_REPLICAS = 30

def get_custom_metric():
    topic = "FORECAST_DOWNLOAD_URLS"
    consumer_group = "FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP"

    total_lag, total_lag_count, lag_per_partition = calculate_consumer_group_lag(topic, consumer_group)
    log(f"Total Lag: {total_lag} across {total_lag_count} partition(s)")
    log(f"Lag Details per Partition: {lag_per_partition}")

    return total_lag, total_lag_count, lag_per_partition

def scale_deployment(replicas):
    if KUBERNETES_OBJECT_TYPE == 'statefulset':
        deployment = apps_v1.read_namespaced_stateful_set(DEPLOYMENT_NAME, NAMESPACE)
        deployment.spec.replicas = replicas
        apps_v1.patch_namespaced_stateful_set(DEPLOYMENT_NAME, NAMESPACE, deployment)
    elif KUBERNETES_OBJECT_TYPE == 'deployment':
        deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
        deployment.spec.replicas = replicas
        apps_v1.patch_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE, deployment)
    log(f"Scaled {DEPLOYMENT_NAME} to {replicas} replicas.")

testbool = True

def checkMetrics():
    global testbool
    metric_value, metric_count, metric_data = get_custom_metric()

    # Read current number of replicas
    if KUBERNETES_OBJECT_TYPE == 'statefulset':
        deployment = apps_v1.read_namespaced_stateful_set(DEPLOYMENT_NAME, NAMESPACE)
    elif KUBERNETES_OBJECT_TYPE == 'deployment':
        deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
    current_replicas = deployment.spec.replicas
    
    # Scaling logic
    if current_replicas < metric_count:
        if current_replicas == 1:
            log(f"Initializing crunching of data with {metric_count} replicas")
            log_producer.produce_message("AUTOSCALER", f"Initializing crunching of data with {metric_count} replicas")
        else:
            log(f"Scaling up from {current_replicas} to {metric_count} replicas")
            log_producer.produce_message("AUTOSCALER", f"Scaling up from {current_replicas} to {metric_count} replicas")
        scale_deployment(metric_count)
        return True
    elif metric_count == 0 and current_replicas > 1:
        log(f"Reducing deployment replicas from {current_replicas} to 1")
        log_producer.produce_message("AUTOSCALER", f"Reducing deployment replicas from {current_replicas} to 1")
        scale_deployment(1)
        return True
    log(f"No scaling performed. current_replicas: {current_replicas}, metric_count: {metric_count}, metric_value: {metric_value}, len(metric_data): {len(metric_data)}")
    return False


def checkMetricsJob():
    while checkMetrics():
        time.sleep(10)

checkMetrics()