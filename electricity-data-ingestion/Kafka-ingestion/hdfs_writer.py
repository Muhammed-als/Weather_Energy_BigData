from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from config import KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL, DatasetConfig
import threading
import time
from datetime import datetime
import json
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
from typing import List, Dict

class HDFSWriter:
    def __init__(self, config: DatasetConfig, record_class):
        self.config = config
        self.record_class = record_class
        # Initialize HDFS client using pyarrow
        self.hdfs = pa.fs.HadoopFileSystem('namenode', port=9000, user='root')
        self.consumer = self.get_consumer()
        self.buffer: List[Dict] = []
        # Increased buffer size for more efficient Parquet writing
        self.flush_size = 10000  # Adjust this based on your memory constraints and requirements
        self.running = False
        self.write_thread = None

        # Parse the schema during initialization
        try:
            self.parsed_schema = json.loads(config.schema)
            print("Schema is valid")
        except Exception as e:
            print(f"Schema is invalid: {e}")
            raise

    def get_consumer(self):
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

        avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry_client,
            schema_str=self.config.schema
        )

        consumer_config = {
            'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP),
            'group.id': self.config.consumer_group,
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': avro_deserializer,
            'auto.offset.reset': 'earliest'
        }

        return DeserializingConsumer(consumer_config)

    def write_to_hdfs(self, records: List[Dict]):
        if not records:
            return

        file_path = f"/Electricitydata.parquet"

        try:
            # Convert records to Arrow table
            table = pa.Table.from_pylist(records)
            
            # Check if file exists
            try:
                existing_table = pq.read_table(file_path, filesystem=self.hdfs)
                # Concatenate with existing data
                table = pa.concat_tables([existing_table, table])
            except:
                # File doesn't exist yet, just use new data
                pass

            # Write to HDFS using Parquet format
            with self.hdfs.open_output_stream(file_path) as out_file:
                pq.write_table(
                    table,
                    out_file,
                    compression='snappy',
                    row_group_size=100000  # Adjust based on your needs
                )
            print(f"Successfully wrote {len(records)} records to HDFS: {file_path}")
        except Exception as e:
            print(f"Error writing to HDFS: {e}")
            raise

    def consume_and_write(self):
        self.consumer.subscribe([self.config.topic])
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                record = msg.value()
                if record is not None:
                    self.buffer.append(record)
                    if len(self.buffer) >= self.flush_size:
                        self.write_to_hdfs(self.buffer)
                        self.buffer = []

            except Exception as e:
                print(f"Error in consumer loop: {e}")

        # Flush any remaining records in the buffer
        if self.buffer:
            self.write_to_hdfs(self.buffer)
        self.consumer.close()

    def start(self):
        self.running = True
        self.write_thread = threading.Thread(target=self.consume_and_write)
        self.write_thread.daemon = True
        self.write_thread.start()
        print(f"Started HDFS writer for topic {self.config.topic}...")

    def stop(self):
        self.running = False
        if self.write_thread:
            self.write_thread.join(timeout=5)
        print(f"Stopped HDFS writer for topic {self.config.topic}.")