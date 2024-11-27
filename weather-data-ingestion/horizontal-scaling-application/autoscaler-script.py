from kubernetes import client, config
import time

# Load the Kubernetes configuration
CONFIG_PATH = 'weather-data-ingestion\horizontal-scaling-application\group-07-kubeconfig'
config.load_kube_config(config_file=CONFIG_PATH)

# API clients
apps_v1 = client.AppsV1Api()
custom_api = client.CustomObjectsApi()

# Constants
NAMESPACE = "group-07"
DEPLOYMENT_NAME = "example-deployment"
METRIC_THRESHOLD = 100  # Example threshold
MIN_REPLICAS = 0
MAX_REPLICAS = 30

def get_custom_metric():
    # Replace this with code to fetch your custom metric (e.g., from Prometheus)
    # Example: Queue size or API request rate
    return 120  # Mock value

def scale_deployment(replicas):
    deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
    deployment.spec.replicas = replicas
    apps_v1.patch_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE, deployment)
    print(f"Scaled {DEPLOYMENT_NAME} to {replicas} replicas.")

while True:
    metric_value = get_custom_metric()
    print(f"Current Metric Value: {metric_value}")

    # Read current number of replicas
    deployment = apps_v1.read_namespaced_deployment(DEPLOYMENT_NAME, NAMESPACE)
    current_replicas = deployment.spec.replicas

    # Scaling logic
    if metric_value > METRIC_THRESHOLD and current_replicas < MAX_REPLICAS:
        scale_deployment(current_replicas + 1)
    elif metric_value < METRIC_THRESHOLD and current_replicas > MIN_REPLICAS:
        scale_deployment(current_replicas - 1)

    # Wait before checking metrics again
    time.sleep(10)
