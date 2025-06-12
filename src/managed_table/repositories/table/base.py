import abc
import datetime

from src.managed_table.domain import value_objects


class AbstractTableRepository(abc.ABC):
    @abc.abstractmethod
    def get_table_metadata(self, table_name: str) -> value_objects.TableMetadata: ...

    @abc.abstractmethod
    def table_exists(self, table_name: str) -> None: ...

    @abc.abstractmethod
    def create_table(  # noqa: PLR0913
        self, table_config: value_objects.TableConfig
    ): ...

    @abc.abstractmethod
    def copy_table(
        self,
        source_table_name: str,
        destination_table_name: str,
        expires: datetime.datetime | None = None,
    ): ...

    @abc.abstractmethod
    def delete_table(self, table_name: str, not_found_ok: bool): ...

    @abc.abstractmethod
    def write_query_results_to_table_partition(
        self, table_name: str, query: str, partition: str
    ): ...

    @abc.abstractmethod
    def write_query_results_to_table(self, table_name: str, query: str): ...

    @abc.abstractmethod
    def format_definition(self, definition: str) -> str: ...
