from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import json

def on_msg_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} partition {msg.partition()} at offset {msg.offset()}')

def on_consume_commit(err, partitions):
    if err:
        print(f"Commit failed: {err}")
    else:
        print(f"Committed offsets: {partitions}")

class KafkaConsumer:
    def __init__(self, kafka_server, schema_registry, topic, groupID, offset):
        self.schema_registry_client = SchemaRegistryClient(schema_registry)

        self.avro_deserializer = AvroDeserializer(
            schema_registry_client=schema_registry
        )

        consumer_conf = {
            'bootstrap.servers': kafka_server,
            'group.id': groupID,
            'auto.offset.reset': offset,
            'on_commit': on_consume_commit
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([topic])

    def consume_message(self):

        msg = self.consumer.poll(10)
        if msg is None:
            print("Error: Consumed message was None")
        
        return msg


class KafkaProducer:
    def __init__(self, kafka_server, schema_registry, topic, avro_schema = None):
        self.schema_registry_client = SchemaRegistryClient(schema_registry)

        # Initialize AvroSerializer with the schema registry client and Avro schema if schema is provided
        if avro_schema is not None:
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                json.dumps(avro_schema)
            )
        else:
            self.avro_serializer = None
        self.string_serializer = StringSerializer('utf-8')
        self.producer = Producer(kafka_server)
        self.topic = topic

    def produce_message(self, key, record) -> bool:
        try:
            # Produce the message to Kafka
            serializedValue = None
            if self.avro_serializer is not None:
                serializedValue = self.avro_serializer(record, SerializationContext(self.topic, MessageField.VALUE))
            else:
                serializedValue = self.string_serializer(record, SerializationContext(self.topic, MessageField.VALUE))


            self.producer.produce(topic=self.topic,
                                    key=self.string_serializer(key), 
                                    value=serializedValue,
                                    on_delivery=on_msg_delivery)
            self.producer.flush()  # Ensure all messages are sent
        except Exception as e:
            print(f"Failed to produce message: {e}")
            return False
        return True