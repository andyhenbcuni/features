from src.managed_table import utils
from src.managed_table.domain import value_objects
from src.managed_table.repositories.config import base

logger = utils.get_logger('table_config_repository')


class InMemoryTableConfigRepository(base.AbstractTableConfigRepository):
    config_registry: dict[str, value_objects.TableConfig] = {}

    def get_table_config(self, table_name: str) -> value_objects.TableConfig:
        try:
            return self.config_registry[table_name]
        except KeyError as e:
            msg = f'TableConfig not found for table name: {table_name}.'
            raise KeyError(msg) from e

    def add_table_config(self, table_config: value_objects.TableConfig) -> None:
        if self.config_registry.get(table_config.table_name):
            logger.warning(
                f'TableConfig already exists for table name: {table_config.table_name}. It will be overwritten.'
            )
        self.config_registry[table_config.table_name] = table_config
