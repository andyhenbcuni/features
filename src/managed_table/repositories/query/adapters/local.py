from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.managed_table import utils
from src.managed_table.repositories.query import base


@dataclass
class InMemoryQueryRepository(base.AbstractQueryRepository):
    registry: dict[str, Callable[[str, dict | None], str]] = field(default_factory=dict)

    def get_query(
        self, query_name: str, run_day: str, run_time_template_fields: dict[str, Any] | None = None
    ) -> str:
        return self.registry[query_name](run_day, run_time_template_fields)

    def get_query_hash(self, query_name: str) -> int:
        query: str = self.get_query(query_name=query_name, run_day='unused')
        return utils.hash_string(string=query)

    def copy_query(self, source_query_name: str, destination_query_name: str) -> None:
        self.registry[destination_query_name] = self.registry[source_query_name]

    def add_query(self, query_name: str, query_renderer: Callable[[str, dict | None], str]) -> None:
        self.registry[query_name] = query_renderer
