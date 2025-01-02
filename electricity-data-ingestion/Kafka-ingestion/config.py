from typing import List, Dict, Type
from dataclasses import dataclass
from models import EnergyPrice, Forecasts5Min

KAFKA_BOOTSTRAP: List[str] = ["kafka:9092"]
SCHEMA_REGISTRY_URL: str = "http://10.152.183.137:8081"
FETCH_INTERVAL: int = 1
ITEMS_PER_PAGE: int = 10000
HISTORICAL_DAYS: int = 8732

@dataclass
class DatasetConfig:
    name: str
    topic: str
    consumer_group: str
    api_endpoint: str
    schema: str
    filter_fields: Dict[str, List[str]]
    sort_field: str
    record_class: Type

# Dataset configurations
DATASETS = {
    "energy_prices": DatasetConfig(
        name="Elspotprices",
        topic="ELECTRICITY_PRICE",
        consumer_group="DEFAULT_CONSUMER",
        api_endpoint="https://api.energidataservice.dk/dataset/{dataset_name}",
        schema="""
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
        """,
        filter_fields={"PriceArea": ["DK1", "DK2"]},
        sort_field="HourUTC desc",
        record_class=EnergyPrice
    )
}