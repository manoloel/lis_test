import asyncio
from dataclasses import dataclass
from enum import Enum, unique

@unique
class CoagTest(Enum):
    PT = 0
    APTT = 1
    TT = 2
    FIBR = 3
    DDIMER = 4
    FACTOR8 = 5
    FACTOR9 = 6
    ANTITR = 7

@dataclass
class TestResult:
    parameter: str # Название параметра ("Время", "Активность", и т.д.)
    value: float # Значение результата
    parameter_unit: str # Единицы измерения

@dataclass
class Settings:
    lis_address: str # IP адрес в формате "адрес:порт"
    device_id: str

@dataclass
class DepContainer:
    settings: Settings
    lis_queue: asyncio.Queue
    version: str
