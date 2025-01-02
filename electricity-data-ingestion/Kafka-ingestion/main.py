import time
from fetcher import APIFetcher
from processor import DataProcessor
from config import DATASETS
from kafka_client import KafkaClient
from hdfs_writer import HDFSWriter

def main():
    processors = []
    hdfs_writers = []
    for dataset_name, config in DATASETS.items():
        record_class = config.record_class
        kafka_client = KafkaClient(config.schema, record_class)
        fetcher = APIFetcher(config)
        processor = DataProcessor(
            kafka_client=kafka_client,
            record_class=record_class,
            config=config
        )
        processor.start(fetcher)
        processors.append(processor)
        
        hdfs_writer = HDFSWriter(config, record_class)
        hdfs_writer.start()
        hdfs_writers.append(hdfs_writer)

    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        for processor in processors:
            processor.stop()
        for hdfs_writer in hdfs_writers:
            hdfs_writer.stop()

if __name__ == "__main__":
    main()