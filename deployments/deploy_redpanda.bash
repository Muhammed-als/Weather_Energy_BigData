#!/bin/bash

echo "========== Deploy Redpanda - Ref: Lecture 3 Exercise 3 =========="
cd "$(dirname "$0")/deployment_files" || exit

kubectl apply -f redpanda.yaml

# Execute commands directly in a new terminal window
# Use "apt-get install konsole" to install konsole if not already.
konsole -e sh -c 'echo "Access Redpanda with: http://127.0.0.1:8080"; echo "Waiting for pod Ready (timeout=20s) . . ."; kubectl wait --for=condition=Ready pod -l app=redpanda --timeout=20s; echo "Start port-forward"; kubectl port-forward svc/redpanda 8080'