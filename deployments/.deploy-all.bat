@echo off

call deploy_kafka_cluster.bat
call deploy_kafka_connect_shema_registry_and_ksql.bat
call deploy_redpanda.bat
call deploy_hdfs_configmap_namenode_datanodes_and_hdfs-cli.bat
call deploy_interactive_pod.bat
call deploy_spark.bat