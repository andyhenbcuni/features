import abc
from collections.abc import Callable
from typing import Any


class AbstractQueryRepository(abc.ABC):
    @abc.abstractmethod
    def get_query(
        self, query_name: str, run_day: str, run_time_template_fields: dict[str, Any] | None = None
    ) -> str: ...

    @abc.abstractmethod
    def get_query_hash(self, query_name: str) -> int: ...

    @abc.abstractmethod
    def copy_query(self, source_query_name: str, destination_query_name: str): ...

    @abc.abstractmethod
    def add_query(
        self, query_name: str, query_renderer: Callable[[str, dict | None], str]
    ) -> None: ...
