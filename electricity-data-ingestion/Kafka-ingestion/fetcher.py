from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
import requests
import json
from config import HISTORICAL_DAYS, ITEMS_PER_PAGE, DatasetConfig

class BaseFetcher(ABC):
    def __init__(self, config: DatasetConfig):
        self.config = config
        self.earliest_date_processed = datetime.now()
        self.latest_date_processed = datetime.now() - timedelta(days=HISTORICAL_DAYS)
        self.fetch_historical = True

    def get_time_window(self) -> Tuple[str, str]:
        now = datetime.now()
        
        if self.fetch_historical:
            start_time = self.earliest_date_processed - timedelta(days=7)
            end_time = self.earliest_date_processed
            self.earliest_date_processed = start_time
            
            if start_time <= self.latest_date_processed:
                self.fetch_historical = False
                print("Completed historical data fetch")
        else:
            start_time = now - timedelta(hours=2)
            end_time = now
        
        return (
            start_time.strftime("%Y-%m-%dT%H:%M"),
            end_time.strftime("%Y-%m-%dT%H:%M")
        )

    @abstractmethod
    def fetch_data(self, start: str, end: str, offset: int = 0) -> Optional[Dict]:
        pass

class APIFetcher(BaseFetcher):
    def fetch_data(self, start: str, end: str, offset: int = 0) -> Optional[Dict]:
        url = self.config.api_endpoint.format(dataset_name=self.config.name)
        
        params = {
            'start': start,
            'end': end,
            'filter': json.dumps(self.config.filter_fields),
            'limit': ITEMS_PER_PAGE,
            'offset': offset,
            'sort': self.config.sort_field
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None