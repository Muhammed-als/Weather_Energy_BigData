# File: kafka_lag_calculator.py

from confluent_kafka.admin import AdminClient, OffsetSpec
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
from src.utils import log, logging

KAFKA_BROKER = "kafka:9092"

def calculate_consumer_group_lag(topic, consumer_group):
    # Initialize AdminClient to get metadata and offsets
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    
    # Get topic metadata
    metadata = admin_client.list_topics(timeout=10)
    if topic not in metadata.topics:
        raise ValueError(f"Topic '{topic}' not found in Kafka cluster.")

    # Fetch partitions for the topic
    partitions = metadata.topics[topic].partitions.keys()

    topic_partitions = []
    for partition in partitions:
        topic_partitions.append(TopicPartition(topic, partition))

    # Fetch log-end offsets (latest offsets)
    log_end_offsets = admin_client.list_offsets({tp: OffsetSpec.latest() for tp in topic_partitions})  # -1 for latest

    # Create a request for consumer group offsets
    cgtps = [ConsumerGroupTopicPartitions(consumer_group, [tp]) for tp in topic_partitions]

    # Fetch consumer group offsets
    consumer_group_offsets = [admin_client.list_consumer_group_offsets([cgtp]) for cgtp in cgtps]
    
    lag_per_partition = {}
    for tp in topic_partitions:
        log_end_offset = log_end_offsets[tp].result().offset
        group_offset_data = consumer_group_offsets[tp.partition][consumer_group].result().topic_partitions[0] ##HERE

        if group_offset_data is None or group_offset_data.offset < 0:
            # If no committed offset is found, default to 0
            group_offset = 0
        else:
            group_offset = group_offset_data.offset

        # Calculate lag for this partition
        lag_per_partition[tp.partition] = [max(0, log_end_offset - group_offset), group_offset]

    # Calculate total lag across all partitions with valid offsets
    total_lag = 0
    total_lag_count = 0
    for values in lag_per_partition.values():
        if values[1] > 0 and values[0] > 0:
            total_lag += values[0]
            total_lag_count += 1

    return total_lag, total_lag_count, lag_per_partition

if __name__ == "__main__":
    topic = "FORECAST_DOWNLOAD_URLS"
    consumer_group = "FORECAST_DOWNLOAD_URLS_CONSUMER_GROUP"

    total_lag, total_lag_count, lag_per_partition = calculate_consumer_group_lag(topic, consumer_group)
    log(f"Total Lag: {total_lag} across {total_lag_count} partition(s)")
    log(f"Lag Details per Partition: {lag_per_partition}")