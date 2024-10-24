#!/bin/bash

echo ========== Port-forward namenode for HDFS Web UI: Lecture 2 Exercise 5 ==========
echo "Reach HDFS Web UI on http://localhost:9870"

kubectl port-forward service/namenode 9870:9870

# Wait for user input (replace 'pause' with interactive command)
read -p "Press Enter to continue..."
