from typing import List, Dict, Type
from dataclasses import dataclass
from models import EnergyPrice, Forecasts5Min

KAFKA_BOOTSTRAP: List[str] = ["kafka:9092"]
SCHEMA_REGISTRY_URL: str = "http://10.152.183.137:8081"
FETCH_INTERVAL: int = 1
ITEMS_PER_PAGE: int = 10000
HISTORICAL_DAYS: int = 365

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
        topic="ENERGY_DATA_AVRO_4",
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
        filter_fields={"PriceArea": ["DK1"]},
        sort_field="HourUTC desc",
        record_class=EnergyPrice
    ),
    "forecasts_5min": DatasetConfig(
        name="Forecasts_5Min",
        topic="FORECASTS_DATA_AVRO",
        consumer_group="FORECASTS_CONSUMER",
        api_endpoint="https://api.energidataservice.dk/dataset/{dataset_name}",
        schema="""
        {
            "namespace": "energydata.avro",
            "type": "record",
            "name": "Forecasts5Min",
            "fields": [
                {"name": "Minutes5UTC", "type": "string"},
                {"name": "Minutes5DK", "type": "string"},
                {"name": "PriceArea", "type": "string"},
                {"name": "ForecastType", "type": "string"},
                {"name": "ForecastDayAhead", "type": ["null", "float"], "default": null},
                {"name": "Forecast5Hour", "type": ["null", "float"], "default": null},
                {"name": "Forecast1Hour", "type": ["null", "float"], "default": null},
                {"name": "ForecastCurrent", "type": ["null", "float"], "default": null},
                {"name": "TimestampUTC", "type": ["null", "string"], "default": null},
                {"name": "TimestampDK", "type": ["null", "string"], "default": null}
            ]
        }
        """,
        filter_fields={"PriceArea": ["DK1"], "ForecastType": ["Solar", "Onshore Wind", "Offshore Wind"]},
        sort_field="Minutes5UTC desc",
        record_class=Forecasts5Min
    )
}
