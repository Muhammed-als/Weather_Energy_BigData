from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import json
from pprint import pprint
from src.utils import *

KAFKA_SERVER = 'kafka:9092'
SCHEMA_REGISTRY = 'http://kafka-schema-registry:8081'

def on_msg_delivery(err, msg):
    if err is not None:
        log(f"Delivery failed for record {msg.key().decode('utf-8')}: {err}")
    else:
        log(f'Record {msg.key().decode('utf-8')} successfully produced to {msg.topic()} partition {msg.partition()} at offset {msg.offset()}')

def on_consume_commit(err, partitions):
    if err:
        log(f"Commit failed: {err}")
    else:
        log(f"Committed offsets:")
        pprint(partitions)

class KafkaConsumer:
    def __init__(self, topic, offset, groupID=None, use_avro = True, avro_schema=None, enable_auto_commit = False):
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY})
        self.topic = topic

        if use_avro:
            if avro_schema is not None:
                self.avro_deserializer = AvroDeserializer(
                    schema_registry_client=self.schema_registry_client,
                    schema_str=json.dumps(avro_schema)
                )
            else:
                self.avro_deserializer = AvroDeserializer(
                    schema_registry_client=self.schema_registry_client
                )
        else:
            self.avro_deserializer = None

        consumer_conf = {
            'bootstrap.servers': KAFKA_SERVER,
            'group.id': 'DEFAULT_CONSUMER' if groupID is None else groupID,
            'auto.offset.reset': offset,
            'on_commit': on_consume_commit,
            'enable.auto.commit': enable_auto_commit
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([self.topic])

    def consume_message(self):
        msg = self.consumer.poll(5)
        
        if msg is None:
            log("Consumed message was None", level=logging.ERROR)
            return None, None, None

        if self.avro_deserializer is not None:
            return (msg, 
                    msg.key().decode('utf-8'), 
                    self.avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)))
        else:
            return (msg, 
                    msg.key().decode('utf-8'), 
                    msg.value().decode('utf-8'))
        
    def close(self):
        self.consumer.close()
        

class KafkaProducer:
    def __init__(self, topic, avro_schema = None):
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY})

        # Initialize AvroSerializer with the schema registry client and Avro schema if schema is provided
        if avro_schema is not None:
            self.avro_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=json.dumps(avro_schema)
            )
        else:
            self.avro_serializer = None
        self.string_serializer = StringSerializer('utf-8')
        self.producer = Producer({'bootstrap.servers': KAFKA_SERVER})
        self.topic = topic

    def produce_message(self, key, record, partition = None) -> bool:
        try:
            # Produce the message to Kafka
            serializedValue = None
            if self.avro_serializer is not None:
                serializedValue = self.avro_serializer(record, SerializationContext(self.topic, MessageField.VALUE))
            else:
                serializedValue = self.string_serializer(record, SerializationContext(self.topic, MessageField.VALUE))

            if partition is not None:
                self.producer.produce(topic=self.topic,
                                        key=self.string_serializer(key), 
                                        value=serializedValue,
                                        partition=partition,
                                        on_delivery=on_msg_delivery)
            else:
                self.producer.produce(topic=self.topic,
                                        key=self.string_serializer(key), 
                                        value=serializedValue,
                                        on_delivery=on_msg_delivery)
            self.producer.flush()  # Ensure all messages are sent
        except Exception as e:
            log(f"Failed to produce message: {e}")
            return False
        return True