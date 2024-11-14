import json
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
import threading

# Configuration
KAFKA_BOOTSTRAP: List[str] = ["kafka:9092"]
DEFAULT_TOPIC: str = "ENERGY_DATA_AVRO_2"
DEFAULT_CONSUMER: str = "DEFAULT_CONSUMER"
API_BASE_URL: str = "https://api.energidataservice.dk/dataset/{dataset_name}"
FETCH_INTERVAL: int = 1  # seconds
ITEMS_PER_PAGE: int = 100  # Number of items to fetch per request
HISTORICAL_DAYS: int = 365  # Days of historical data to fetch
SCHEMA_REGISTRY_URL: str = "http://10.152.183.137:8081"

# Avro schema definition
value_schema_str = """
{
    "namespace": "energydata.avro",
    "type": "record",
    "name": "EnergyPrice",
    "fields": [
        {"name": "HourUTC", "type": "string"},
        {"name": "HourDK", "type": "string"},
        {"name": "PriceArea", "type": "string"},
        {"name": "SpotPriceDKK", "type": "float"},
        {"name": "SpotPriceEUR", "type": "float"}
    ]
}
"""

@dataclass
class EnergyPrice:
    HourUTC: str
    HourDK: str
    PriceArea: str
    SpotPriceDKK: float
    SpotPriceEUR: float

    def to_dict(self) -> Dict:
        return {
            "HourUTC": self.HourUTC,
            "HourDK": self.HourDK,
            "PriceArea": self.PriceArea,
            "SpotPriceDKK": self.SpotPriceDKK,
            "SpotPriceEUR": self.SpotPriceEUR
        }

    def __hash__(self):
        return hash(f"{self.HourUTC}_{self.PriceArea}")

class EnergyDataFetcher:
    # EnergyDataFetcher class remains the same
    def __init__(self, dataset_name: str, price_area: str):
        self.dataset_name = dataset_name
        self.price_area = price_area
        self.processed_records: Set[str] = set()
        self.earliest_date_processed = datetime.now()
        self.latest_date_processed = datetime.now() - timedelta(days=HISTORICAL_DAYS)
        self.fetch_historical = True

    def fetch_data(self, start: str, end: str, offset: int = 0) -> Optional[Dict]:
        """Fetch energy data from the API with pagination"""
        url = API_BASE_URL.format(dataset_name=self.dataset_name)
        
        params = {
            'start': start,
            'end': end,
            'filter': json.dumps({"PriceArea": [self.price_area]}),
            'limit': ITEMS_PER_PAGE,
            'offset': offset,
            'sort': 'HourUTC desc'  # Sort by newest first
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def get_time_window(self) -> tuple[str, str]:
        """Get the time window for the next data fetch"""
        now = datetime.now()
        
        if self.fetch_historical:
            # Fetch historical data in 7-day chunks
            start_time = self.earliest_date_processed - timedelta(days=7)
            end_time = self.earliest_date_processed
            
            # Update the earliest date processed
            self.earliest_date_processed = start_time
            
            # Check if we've reached our historical limit
            if start_time <= self.latest_date_processed:
                self.fetch_historical = False
                print("Completed historical data fetch")
        else:
            # Fetch recent data
            start_time = now - timedelta(hours=2)
            end_time = now
        
        return (
            start_time.strftime("%Y-%m-%dT%H:%M"),
            end_time.strftime("%Y-%m-%dT%H:%M")
        )

class EnergyDataProcessor:
    def __init__(self, dataset_name: str = "Elspotprices", price_area: str = "DK1"):
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.producer = self.get_producer()
        self.consumer = self.get_consumer(DEFAULT_TOPIC)
        self.fetcher = EnergyDataFetcher(dataset_name, price_area)
        self.running = False
        self.fetch_thread = None

    def avro_to_energy_price(self, avro_data: Dict) -> EnergyPrice:
        """Convert Avro data to EnergyPrice object"""
        return EnergyPrice(
            HourUTC=avro_data['HourUTC'],
            HourDK=avro_data['HourDK'],
            PriceArea=avro_data['PriceArea'],
            SpotPriceDKK=avro_data['SpotPriceDKK'],
            SpotPriceEUR=avro_data['SpotPriceEUR']
        )

    def dict_to_avro(self, obj: Dict, ctx) -> Dict:
        """Convert dictionary to Avro format"""
        return obj

    def get_producer(self) -> SerializingProducer:
        """Create a SerializingProducer with Avro serialization"""
        avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=value_schema_str,
            to_dict=self.dict_to_avro
        )

        producer_config = {
            'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP),
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
            'compression.type': 'lz4',
            'retries': 5,
            'acks': 'all'
        }

        return SerializingProducer(producer_config)

    def get_consumer(self, topic: str, group_id: str = None) -> DeserializingConsumer:
        """Create a DeserializingConsumer with Avro deserialization"""
        if group_id is None:
            group_id = DEFAULT_CONSUMER

        avro_deserializer = AvroDeserializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=value_schema_str,
            from_dict=self.avro_to_energy_price
        )

        consumer_config = {
            'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP),
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': avro_deserializer,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'request.timeout.ms': 40000,
            'max.poll.interval.ms': 300000
        }

        return DeserializingConsumer(consumer_config)

    def consume_messages(self, callback=None):
        """Consume messages from Kafka topic with error handling"""
        self.consumer.subscribe([DEFAULT_TOPIC])

        while self.running:
            try:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                energy_price = msg.value()
                if callback:
                    callback(energy_price)
                else:
                    print(f"Received energy price data: {energy_price}")

            except Exception as e:
                print(f"Error in message consumption: {e}")
                time.sleep(1)

            if not self.running:
                break

    def send_messages(self, energy_prices: List[EnergyPrice]) -> None:
        """Send energy price data to Kafka using Avro serialization"""
        if not energy_prices:
            return

        def delivery_report(err, msg):
            if err is not None:
                print(f'Message delivery failed: {err}')
            else:
                print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

        for price in energy_prices:
            try:
                key = datetime.strptime(price.HourUTC, "%Y-%m-%dT%H:%M:%S").timestamp()
                self.producer.produce(
                    topic=DEFAULT_TOPIC,
                    key=str(key),
                    value=price.to_dict(),
                    on_delivery=delivery_report
                )
                self.producer.poll(0)  # Trigger delivery reports

            except Exception as e:
                print(f"Error producing message: {e}")

        self.producer.flush()
        print(f"Sent {len(energy_prices)} messages to Kafka")

    def process_energy_data(self, data: Dict) -> List[EnergyPrice]:
        """Process the raw energy data and convert it to EnergyPrice objects"""
        energy_prices = []
        new_records = 0
        
        for record in data.get('records', []):
            energy_price = EnergyPrice(
                HourUTC=record['HourUTC'],
                HourDK=record['HourDK'],
                PriceArea=record['PriceArea'],
                SpotPriceDKK=record['SpotPriceDKK'],
                SpotPriceEUR=record['SpotPriceEUR']
            )
            
            # Check if we've already processed this record
            record_id = f"{energy_price.HourUTC}_{energy_price.PriceArea}"
            if record_id not in self.fetcher.processed_records:
                energy_prices.append(energy_price)
                self.fetcher.processed_records.add(record_id)
                new_records += 1

        if new_records > 0:
            print(f"Found {new_records} new records for time window")
            
        return energy_prices

    def fetch_all_pages(self, start_time: str, end_time: str) -> List[EnergyPrice]:
        """Fetch all pages of data for the given time window"""
        all_prices = []
        offset = 0
        total_records = None
        
        while True:
            data = self.fetcher.fetch_data(start_time, end_time, offset)
            if not data:
                break
                
            if total_records is None:
                total_records = data.get('total', 0)
                print(f"Total records available: {total_records} for window {start_time} to {end_time}")
            
            prices = self.process_energy_data(data)
            all_prices.extend(prices)
            
            if len(data.get('records', [])) < ITEMS_PER_PAGE:
                break
                
            offset += ITEMS_PER_PAGE
            
        return all_prices

    def continuous_fetch(self):
        """Continuously fetch new data"""
        while self.running:
            try:
                start_time, end_time = self.fetcher.get_time_window()
                print(f"\nFetching data from {start_time} to {end_time}")
                
                # Fetch all pages for the current time window
                energy_prices = self.fetch_all_pages(start_time, end_time)
                
                # Send new prices to Kafka
                if energy_prices:
                    self.send_messages(energy_prices)
                
                # If we're doing historical fetch, don't wait
                if not self.fetcher.fetch_historical:
                    time.sleep(FETCH_INTERVAL)
                
            except Exception as e:
                print(f"Error in continuous fetch: {e}")
                time.sleep(FETCH_INTERVAL)

    def start(self):
        """Start continuous data fetching"""
        self.running = True
        self.fetch_thread = threading.Thread(target=self.continuous_fetch)
        self.fetch_thread.daemon = True
        self.fetch_thread.start()
        print("Started continuous data fetching...")

    def stop(self):
        """Stop continuous data fetching"""
        self.running = False
        if self.fetch_thread:
            self.fetch_thread.join(timeout=5)  # Add timeout to thread joining
        try:
            self.producer.close(timeout=5)
            self.consumer.close(autocommit=True)
        except Exception as e:
            print(f"Error during shutdown: {e}")
        print("Stopped data fetching and closed connections.")

def main():
    processor = None
    try:
        processor = EnergyDataProcessor(
            dataset_name="Elspotprices",
            price_area="DK1"
        )
        
        processor.start()
        
        def handle_message(energy_price):
            print(f"Processing price: {energy_price.SpotPriceDKK} DKK at {energy_price.HourDK}")
        
        processor.consume_messages(callback=handle_message)
    
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if processor:
            processor.stop()

if __name__ == "__main__":
    main()