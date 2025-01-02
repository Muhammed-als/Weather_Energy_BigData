import time
from fetcher import APIFetcher
from processor import DataProcessor
from config import DATASETS
from kafka_client import KafkaClient

def main():
    processors = []
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

    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        for processor in processors:
            processor.stop()

if __name__ == "__main__":
    main()
