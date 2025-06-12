import datetime
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo

from src.common import environment
from src.managed_table import utils
from src.managed_table.domain import value_objects
from src.managed_table.repositories.table import base, exceptions

logger = utils.get_logger(name='unity_catalog_table_repository')


def default_client() -> WorkspaceClient:
    return WorkspaceClient()


class QueryReturnedNoDataError(Exception):
    pass


@dataclass
class UnityCatalogTableRepository(base.AbstractTableRepository):
    client: WorkspaceClient = field(default_factory=default_client)

    def get_table_metadata(self, table_name: str) -> value_objects.TableMetadata:
        try:
            return value_objects.TableMetadata(
                table_name=table_name,
                schema=self._get_schema(table_name=table_name),
                partition_field=self._get_partition_field(table_name=table_name),
                partitions=self._get_partitions(table_name=table_name),
                definition=self._get_definition(table_name=table_name),
                created=self._get_creation_time(table_name=table_name),
                updated=self._get_last_update_time(table_name=table_name),
            )
        except Exception as e:
            msg: str = f'Table: {table_name} does not exist in Unity Catalog.'
            raise exceptions.TableDoesNotExistError(msg) from e

    def _get_table(self, table_name: str) -> TableInfo:
        full_table_name: str = self._convert_to_full_table_name(table_name=table_name)
        table_info: TableInfo = self.client.tables.get(table_name=full_table_name)
        return table_info

    @staticmethod
    def _convert_to_full_table_name(table_name: str) -> str:
        return environment.DB_DESTINATION.format(table_name=table_name)

    def table_exists(self, table_name: str) -> None:
        try:
            _: TableInfo = self._get_table(table_name=table_name)
        except Exception as e:
            msg = f'Table {table_name} does not exists in Unity Catalog'
            raise exceptions.TableDoesNotExistError(msg) from e

    def _get_schema(self, table_name: str) -> list[dict[str, Any]]:
        table_info = self._get_table(table_name=table_name)
        if table_info and table_info.columns:
            return [{'name': col.name, 'type': col.type_text} for col in table_info.columns]
        return []

    def _get_partition_field(self, table_name: str) -> str:
        table_info = self._get_table(table_name=table_name)

        if table_info is None or not table_info.columns:
            raise ValueError(f'Table {table_name} not found in Unity Catalog')

        return table_info.columns[0].name or ''

    def _get_partitions(self, table_name: str) -> list[str]:
        table_info = self._get_table(table_name=table_name)

        if hasattr(table_info, 'columns') and table_info.columns:
            return [col.name for col in table_info.columns if col.name]
        return []

    def _get_creation_time(self, table_name: str) -> datetime.datetime:
        table_info: TableInfo = self._get_table(table_name=table_name)

        if table_info.created_at is None:
            msg = f'Table: {table_name} has no creation timestamp.'
            raise NotImplementedError(msg)

        return datetime.datetime.fromtimestamp(table_info.created_at / 1000)

    def _get_last_update_time(self, table_name: str) -> datetime.datetime:
        table_info: TableInfo = self._get_table(table_name=table_name)

        if table_info.updated_at is None:
            msg = f'Table: {table_name} has no timestamp for its last update.'
            raise NotImplementedError(msg)

        return datetime.datetime.fromtimestamp(table_info.updated_at / 1000)

    def _get_definition(self, table_name: str) -> str:
        if table_info := self._get_table(table_name=table_name):
            return table_info.comment or ''
        return ''

    def create_table(self, table_config: value_objects.TableConfig) -> None:
        create_sql = f"""
        CREATE TABLE {table_config.table_name} (
            {", ".join(f"{col['name']} {col['type']}" for col in table_config.schema)}
        )
        {f"PARTITIONED BY ({table_config.partition_field})" if table_config.partition_field else ""}
        COMMENT '{table_config.definition}'
        """.strip()

        self.client.statement_execution.execute(create_sql)

    def copy_table(
        self,
        source_table_name: str,
        destination_table_name: str,
        expires: datetime.datetime | None = None,
    ) -> None:
        copy_sql = f'CREATE TABLE {destination_table_name} AS SELECT * FROM {source_table_name}'
        self.client.statement_execution.execute(copy_sql)

    def delete_table(self, table_name: str, not_found_ok: bool = False) -> None:
        try:
            self.client.tables.delete(table_name)
        except Exception as e:
            if not_found_ok:
                logger.warning(f'Table {table_name} not found, skipping deletion.')
            else:
                raise e

    def write_query_results_to_table_partition(
        self, table_name: str, query: str, partition: str
    ) -> None:
        insert_sql = f'INSERT INTO {table_name} PARTITION ({partition}) {query}'
        self.client.statement_execution.execute(insert_sql)

    def write_query_results_to_table(self, table_name: str, query: str) -> None:
        insert_sql = f'INSERT INTO {table_name} {query}'
        self.client.statement_execution.execute(insert_sql)

    def format_definition(self, definition: str) -> str:
        return str(utils.hash_string(string=definition))[:63]
