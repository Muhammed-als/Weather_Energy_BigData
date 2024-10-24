@echo off

echo ========== Deploy HDFS - Ref: services\hdfs ==========
cd /d %~dp0\deployment_files
kubectl apply -f configmap.yaml
kubectl apply -f namenode.yaml
echo Waiting for namenode condition Ready (timeout=10s) . . .
kubectl wait --for=condition=Ready pod -l app=namenode --timeout=20s
kubectl apply -f datanodes.yaml
:: Optional hdfs-cli
kubectl apply -f hdfs-cli.yaml

pause