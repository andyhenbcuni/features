import abc

from src.managed_table.domain import value_objects


class AbstractTableConfigRepository(abc.ABC):
    @abc.abstractmethod
    def get_table_config(self, table_name: str) -> value_objects.TableConfig: ...

    @abc.abstractmethod
    def add_table_config(self, table_config: value_objects.TableConfig) -> None: ...
