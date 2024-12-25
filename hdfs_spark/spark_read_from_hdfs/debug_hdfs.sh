#!/bin/bash

echo "Checking HDFS connectivity..."
hdfs dfsadmin -report

echo "Listing root directory in HDFS..."
hdfs dfs -ls /

echo "Fetching data sample from HDFS..."
hdfs dfs -cat /path/to/sample.parquet | head -n 10
