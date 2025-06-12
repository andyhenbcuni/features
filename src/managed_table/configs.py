from dataclasses import dataclass

from src.managed_table.repositories.config.adapters import local as local_config_repo
from src.managed_table.repositories.config.base import AbstractTableConfigRepository
from src.managed_table.repositories.query.adapters import local as local_query_repo
from src.managed_table.repositories.query.base import AbstractQueryRepository
from src.managed_table.repositories.table.adapters import bigquery
from src.managed_table.repositories.table.base import AbstractTableRepository


@dataclass(frozen=True)
class BootstrapConfig:
    query_repository: AbstractQueryRepository
    table_repository: AbstractTableRepository
    table_config_repository: AbstractTableConfigRepository | None = None


DEFAULT_CONFIG = BootstrapConfig(
    query_repository=local_query_repo.InMemoryQueryRepository(),
    table_repository=bigquery.BigQueryTableRepository(),
    table_config_repository=local_config_repo.InMemoryTableConfigRepository(),
)
