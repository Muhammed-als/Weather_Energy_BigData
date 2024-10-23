@echo off

echo ========== Deploy a Kafka cluster - Ref: Lecture 3 Exercise 1 ==========
cd /d %~dp0\Kafka_deployment_files
helm install --values kafka-values.yaml kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 30.0.4
kubectl run kafka-client --restart=Never --image docker.io/bitnami/kafka:3.8.0-debian-12-r3  --command -- sleep infinity

pause