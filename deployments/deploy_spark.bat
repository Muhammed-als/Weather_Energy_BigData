@echo off

echo ========== Deploy a Kafka cluster - Ref: Lecture 4 Exercise 1 ==========
cd /d %~dp0\deployment_files
helm install --values spark-values.yaml spark oci://registry-1.docker.io/bitnamicharts/spark --version 9.2.10
kubectl port-forward svc/spark-master-svc 8080:80
pause