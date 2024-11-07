#!/bin/bash

echo ========== Deploy a Kafka cluster - Ref: Lecture 4 Exercise 1 ==========
cd "$(dirname "$0")/deployment_files"

# Install Spark with Helm using values file
helm install --values spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.10

# Inspect the UI of the Spark
kubectl port-forward svc/spark-master-svc 8080:80