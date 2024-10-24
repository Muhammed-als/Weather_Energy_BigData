#!/bin/bash

echo ========== Deploy a Kafka Connect, Kafka Schema Registry, and Kafka KSQL - Ref: Lecture 3 Exercise 2 ==========
cd "$(dirname "$0")/deployment_files"
kubectl apply -f kafka-schema-registry.yaml 
kubectl apply -f kafka-connect.yaml
kubectl apply -f kafka-ksqldb.yaml

read -p "Press Enter to continue..."