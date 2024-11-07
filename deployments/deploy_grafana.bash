#!/bin/bash

echo "========== Deploy Grafana =========="
cd "$(dirname "$0")/deployment_files" || exit

kubectl apply -f grafana.yaml

# Execute commands directly in a new terminal window
# Use "apt-get install konsole" to install konsole if not already.
konsole -e sh -c 'echo "Access Grafana with: http://127.0.0.1:3000"; echo "Waiting for pod Ready (timeout=20s) . . ."; kubectl wait --for=condition=Ready pod -l app=grafana --timeout=20s; echo "Start port-forward"; kubectl port-forward service/grafana 3000'