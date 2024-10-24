#!/bin/bash

echo ========== Deploy interactive pod - Ref: services\interactive - Alt. apache/hadoop:3 Ref: lecture 2 Exercise 2 ==========
echo Opening separate terminal window with deployments...

cd "$(dirname "$0")/deployment_files"

# Deploy interactive pod
kubectl run interactive -i --tty --image registry.gitlab.sdu.dk/jah/bigdatarepo/interactive:latest -- /bin/bash &

# Deploy interactive Hadoop 3 pod (uncomment and adjust image if needed)
# kubectl run hadoop3 -i --tty --image apache/hadoop:3 -- /bin/bash &

# Wait for user input (replace 'pause' with interactive command)
read -p "Press Enter to continue..."