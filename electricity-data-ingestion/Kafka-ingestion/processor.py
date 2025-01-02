from typing import Dict, List, Type
from kafka_client import KafkaClient
from models import BaseRecord
import threading
import time
from datetime import datetime
from config import FETCH_INTERVAL, ITEMS_PER_PAGE, DatasetConfig

class DataProcessor:
    def __init__(
        self,
        kafka_client: KafkaClient,
        record_class: Type[BaseRecord],
        config: DatasetConfig
    ):
        self.kafka_client = kafka_client
        self.record_class = record_class
        self.config = config
        self.producer = kafka_client.get_producer()
        self.running = False
        self.fetch_thread = None
        self.processed_records = set()
        self.message_count = 0

    def process_data(self, data: Dict) -> List[BaseRecord]:
        records = []
        new_records = 0
        
        for record in data.get('records', []):
            parsed_record = self.record_class.from_dict(record)
            record_id = parsed_record.get_key()
            
            if record_id not in self.processed_records:
                records.append(parsed_record)
                self.processed_records.add(record_id)
                new_records += 1

        if new_records > 0 and new_records % 100 == 0:
            print(f"Found {new_records} new records")
            
        return records

    def send_messages(self, messages: List[BaseRecord]) -> None:
        if not messages:
            return

        def delivery_report(err, msg):
            if err is not None:
                print(f'Message delivery failed: {err}')
            else:
                self.message_count += 1
                if self.message_count % 1000 == 0:
                    print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

        for message in messages:
            try:
                self.producer.produce(
                    topic=self.config.topic,
                    key=message.get_key(),
                    value=message.to_dict(),
                    on_delivery=delivery_report
                )
                self.producer.poll(0)
            except Exception as e:
                print(f"Error producing message: {e}")

        self.producer.flush()
        print(f"Sent {len(messages)} messages to Kafka")

    def fetch_all_pages(self, fetcher, start_time: str, end_time: str) -> List[BaseRecord]:
        all_data = []
        offset = 0
        
        while True:
            data = fetcher.fetch_data(start_time, end_time, offset)
            if not data:
                break
                
            processed_data = self.process_data(data)
            all_data.extend(processed_data)
            
            if len(data.get('records', [])) < ITEMS_PER_PAGE:
                break
                
            offset += ITEMS_PER_PAGE
            
        return all_data

    def continuous_fetch(self, fetcher):
        while self.running:
            try:
                start_time, end_time = fetcher.get_time_window()
                print(f"\nFetching data from {start_time} to {end_time}")
                
                data = self.fetch_all_pages(fetcher, start_time, end_time)
                
                if data:
                    self.send_messages(data)
                
                if not fetcher.fetch_historical:
                    time.sleep(FETCH_INTERVAL)
                
            except Exception as e:
                print(f"Error in continuous fetch: {e}")
                time.sleep(FETCH_INTERVAL)

    def start(self, fetcher):
        self.running = True
        self.fetch_thread = threading.Thread(target=self.continuous_fetch, args=(fetcher,))
        self.fetch_thread.daemon = True
        self.fetch_thread.start()
        print("Started continuous data fetching...")

    def stop(self):
        self.running = False
        if self.fetch_thread:
            self.fetch_thread.join(timeout=5)
        self.producer.flush(timeout=5)
        print("Stopped data fetching and closed connections.")