@echo off

echo "Reach Grafana Web UI on http://localhost:3000
kubectl port-forward service/grafana 3000:3000

pause