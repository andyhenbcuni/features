import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from src.pipelines import port


@dataclass
class Task(port.Port):
    """a task is an action with a name and dependencies"""

    name: str
    action: Callable[..., Any]
    parameters: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    src: Path = Path(__file__).parent
    retries: int = 3
    retry_delay: timedelta = timedelta(minutes=1)

    def __eq__(self, other: object):
        if isinstance(other, type(self)) and all(
            [
                self.name == other.name,
                inspect.getsource(self.action) == inspect.getsource(other.action),
                self.parameters == other.parameters,
                self.depends_on == other.depends_on,
                self.retries == other.retries,
                self.retry_delay == other.retry_delay,
            ]
        ):
            return True
        return False
