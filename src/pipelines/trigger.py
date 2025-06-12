from dataclasses import dataclass

from src.pipelines import port


@dataclass
class Trigger(port.Port): ...


@dataclass
class CronTrigger(Trigger):
    schedule: str
    start_date: str
