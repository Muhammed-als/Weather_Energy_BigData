from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from config import KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL
from typing import Dict
class KafkaClient:
    def __init__(self, schema_str: str, value_type: type):
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.schema_str = schema_str
        self.value_type = value_type

    def dict_to_avro(self, obj: Dict, ctx) -> Dict:
        return obj

    def get_producer(self) -> SerializingProducer:
        avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self.schema_str,
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
