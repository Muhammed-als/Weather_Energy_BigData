#!/bin/bash

echo ========== Deploy a Kafka cluster - Ref: Lecture 3 Exercise 1 ==========
cd "$(dirname "$0")/deployment_files"

# Install Kafka with Helm using values file
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --values kafka-values.yaml \
  --version 30.0.4

# Run a Kafka client container (modify image/version if needed)
kubectl run kafka-client --restart=Never \
  --image docker.io/bitnami/kafka:3.8.0-debian-12-r3 \
  --command -- sleep infinity

# Wait for user input (replace 'pause' with interactive command)
read -p "Press Enter to continue..."