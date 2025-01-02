from dataclasses import dataclass
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

class BaseRecord(ABC):
    @abstractmethod
    def to_dict(self) -> Dict:
        pass

    @abstractmethod
    def get_key(self) -> str:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, record: Dict) -> 'BaseRecord':
        pass

@dataclass
class EnergyPrice(BaseRecord):
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

    def get_key(self) -> str:
        return f"{self.HourUTC}_{self.PriceArea}"

    @classmethod
    def from_dict(cls, record: Dict) -> 'EnergyPrice':
        return cls(
            HourUTC=record['HourUTC'],
            HourDK=record['HourDK'],
            PriceArea=record['PriceArea'],
            SpotPriceDKK=record['SpotPriceDKK'],
            SpotPriceEUR=record['SpotPriceEUR']
        )

@dataclass
class Forecasts5Min(BaseRecord):
    Minutes5UTC: str
    Minutes5DK: str
    PriceArea: str
    ForecastType: str
    ForecastDayAhead: Optional[float] = None
    Forecast5Hour: Optional[float] = None
    Forecast1Hour: Optional[float] = None
    ForecastCurrent: Optional[float] = None
    TimestampUTC: Optional[str] = None
    TimestampDK: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "Minutes5UTC": self.Minutes5UTC,
            "Minutes5DK": self.Minutes5DK,
            "PriceArea": self.PriceArea,
            "ForecastType": self.ForecastType,
            "ForecastDayAhead": self.ForecastDayAhead,
            "Forecast5Hour": self.Forecast5Hour,
            "Forecast1Hour": self.Forecast1Hour,
            "ForecastCurrent": self.ForecastCurrent,
            "TimestampUTC": self.TimestampUTC,
            "TimestampDK": self.TimestampDK
        }

    def get_key(self) -> str:
        return f"{self.Minutes5UTC}_{self.PriceArea}_{self.ForecastType}"

    @classmethod
    def from_dict(cls, record: Dict) -> 'Forecasts5Min':
        return cls(
            Minutes5UTC=record['Minutes5UTC'],
            Minutes5DK=record['Minutes5DK'],
            PriceArea=record['PriceArea'],
            ForecastType=record['ForecastType'],
            ForecastDayAhead=record.get('ForecastDayAhead'),
            Forecast5Hour=record.get('Forecast5Hour'),
            Forecast1Hour=record.get('Forecast1Hour'),
            ForecastCurrent=record.get('ForecastCurrent'),
            TimestampUTC=record.get('TimestampUTC'),
            TimestampDK=record.get('TimestampDK')
        )