#!/bin/bash

echo "Reach Grafana Web UI on http://localhost:3000"

kubectl port-forward service/grafana 3000:3000

# Wait for user input (replace 'pause' with interactive command)
read -p "Press Enter to continue..."
