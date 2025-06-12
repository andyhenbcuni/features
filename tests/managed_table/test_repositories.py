import datetime
from collections.abc import Callable
from contextlib import nullcontext as does_not_raise

import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import table as bq_table

from src.managed_table import utils
from src.managed_table.domain import value_objects
from src.managed_table.repositories.config.adapters import local as local_config_repo
from src.managed_table.repositories.query.adapters import local
from src.managed_table.repositories.table import exceptions
from src.managed_table.repositories.table.adapters import bigquery
from tests.managed_table import helpers


class TestBigQueryTableRepository:
    table_repository = bigquery.BigQueryTableRepository(
        client=bq_client.Client(project='nbcu-ds-algo-int-001')
    )

    @pytest.mark.network
    def test_can_get_table_metadata_for_existing_table(self, empty_partitioned_table, uid) -> None:
        expected_table_name = f'test_table_{uid}'

        expected_schema = [('column_1', 'TIMESTAMP'), ('column_2', 'INTEGER')]
        expected_partition_field = 'column_1'
        expected_partitions = []
        expected_definition = 'test_table_definition'

        _ = empty_partitioned_table(
            table_name=expected_table_name,
            schema=expected_schema,
            partition_field=expected_partition_field,
            labels={'definition': expected_definition},
        )

        actual_metadata: value_objects.TableMetadata | None = (
            self.table_repository.get_table_metadata(table_name=expected_table_name)
        )

        assert actual_metadata.table_name == expected_table_name
        assert actual_metadata.partition_field == expected_partition_field
        assert actual_metadata.partitions == expected_partitions
        assert actual_metadata.definition == expected_definition
        assert actual_metadata.schema == expected_schema

    @pytest.mark.network
    def test_raises_does_not_exist_if_table_metadata_not_found(self) -> None:
        with pytest.raises(expected_exception=exceptions.TableDoesNotExistError):
            _: value_objects.TableMetadata | None = self.table_repository.get_table_metadata(
                table_name='non_existent_table'
            )

    @pytest.mark.network
    def test_can_create_table(self) -> None:
        client = bq_client.Client()
        expected_table_name = 'test_table'
        expected_schema = [('column_1', 'TIMESTAMP'), ('column_2', 'INTEGER')]
        expected_partition_field = 'column_1'
        definition = 'test_table_definition'

        self.table_repository.create_table(
            table_config=helpers.get_table_config(
                table_name=expected_table_name,
                schema=expected_schema,
                partition_field=expected_partition_field,
                definition=definition,
            )
        )

        actual_table: bq_table.Table = client.get_table(
            table=self.table_repository._convert_table_name_to_id(table_name=expected_table_name)
        )

        assert actual_table.table_id == expected_table_name
        assert actual_table.time_partitioning.field == expected_partition_field
        assert actual_table.schema == self.table_repository._convert_schema_to_schema_fields(
            schema=expected_schema
        )
        assert actual_table.labels == {'definition': definition}

        client.delete_table(table=actual_table, not_found_ok=True)

    @pytest.mark.network
    def test_can_copy_table(self, empty_partitioned_table, uid) -> None:
        client = bq_client.Client()
        source_table_name = f'source_table_{uid}'
        destination_table_name = f'destination_table_{uid}'
        definition = 'test_table_definition'

        table_1 = empty_partitioned_table(
            table_name=source_table_name,
            schema=[('partition_field_1', 'DATE')],
            partition_field='partition_field_1',
            labels={'definition': definition},
        )

        self.table_repository.copy_table(
            source_table_name=table_1.table_id, destination_table_name=destination_table_name
        )

        actual_table: bq_table.Table = client.get_table(
            table=self.table_repository._convert_table_name_to_id(table_name=destination_table_name)
        )

        assert actual_table.table_id == destination_table_name
        assert actual_table.time_partitioning.field == table_1.time_partitioning.field
        assert actual_table.schema == table_1.schema
        assert actual_table.labels == {'definition': definition}

        client.delete_table(
            table=self.table_repository._convert_table_name_to_id(
                table_name=destination_table_name
            ),
            not_found_ok=True,
        )

    @pytest.mark.network
    def test_can_delete_table(self, empty_partitioned_table, uid) -> None:
        client = bq_client.Client()
        table_name = f'test_table_{uid}'
        table = empty_partitioned_table(
            table_name=table_name,
            schema=[('partition_field_1', 'DATE')],
            partition_field='partition_field_1',
        )

        self.table_repository.delete_table(table_name=table.table_id)

        with pytest.raises(expected_exception=google_exceptions.NotFound):
            client.get_table(
                table=self.table_repository._convert_table_name_to_id(table_name=table_name)
            )

    @pytest.mark.network
    def test_can_write_query_results_to_table_partition(self, empty_partitioned_table, uid):
        client = bq_client.Client()
        table_name = f'test_partitioned_table_{uid}'

        table = empty_partitioned_table(
            table_name=table_name,
            schema=[('partition_field', 'DATE'), ('number', 'INTEGER')],
            partition_field='partition_field',
        )
        partition = '2024-01-01'

        self.table_repository.write_query_results_to_table_partition(
            table_name=table.table_id,
            query=f"SELECT DATE('{partition}') AS partition_field, 1 AS number",
            partition=partition,
        )

        actual_results = next(
            client.query(
                query=f"SELECT * FROM {self.table_repository._convert_table_name_to_id(table_name=table.table_id)} WHERE partition_field = '{partition}'"
            ).result()
        )

        assert actual_results['partition_field'] == datetime.date(year=2024, month=1, day=1)
        assert actual_results['number'] == 1

    @pytest.mark.network
    def test_can_write_query_results_to_table(self, empty_table, uid):
        client = bq_client.Client()
        table_name = f'test_table_{uid}'

        table = empty_table(
            table_name=table_name,
            schema=[('number', 'INTEGER')],
        )

        self.table_repository.write_query_results_to_table(
            table_name=table.table_id,
            query='SELECT 1 AS number',
        )

        actual_results = next(
            client.query(
                query=f'SELECT * FROM {self.table_repository._convert_table_name_to_id(table_name=table.table_id)}'
            ).result()
        )

        assert actual_results['number'] == 1


class TestInMemoryTableConfigRepository:
    def test_get_table_config_can_get_existing_config(self):
        repo = local_config_repo.InMemoryTableConfigRepository()
        stub_table_name = 'stub_table_name'
        stub_config: value_objects.TableConfig = helpers.get_table_config(
            table_name=stub_table_name
        )
        repo.config_registry = {stub_table_name: stub_config}

        assert repo.get_table_config(table_name=stub_table_name) == stub_config

    def test_get_table_config_raises_if_config_does_not_exist(self):
        repo = local_config_repo.InMemoryTableConfigRepository()
        with pytest.raises(expected_exception=KeyError, match='TableConfig not found .*'):
            repo.get_table_config(table_name='missing_table_name')

    def test_add_table_config_can_write_when_no_key_exists(self):
        repo = local_config_repo.InMemoryTableConfigRepository()
        stub_table_name = 'stub_table_name'
        stub_config: value_objects.TableConfig = helpers.get_table_config(
            table_name=stub_table_name
        )

        repo.add_table_config(table_config=stub_config)

        assert repo.config_registry[stub_table_name] == stub_config

    def test_add_table_config_can_overwrite_existing(self):
        repo = local_config_repo.InMemoryTableConfigRepository()
        stub_table_name = 'stub_table_name'
        stub_config: value_objects.TableConfig = helpers.get_table_config(
            table_name=stub_table_name
        )
        repo.config_registry = {stub_table_name: stub_config}

        with does_not_raise():
            repo.add_table_config(table_config=stub_config)


class TestInMemoryQueryRepository:
    def test_can_get_query(self) -> None:
        stub_query = 'stub_query'
        expected_query = 'stub_value'
        stub_registry: dict[str, Callable[[str], str]] = {stub_query: lambda *args: expected_query}

        repo = local.InMemoryQueryRepository(registry=stub_registry)

        actual_query: str = repo.get_query(query_name=stub_query, run_day='unused')

        assert actual_query == expected_query

    def test_can_get_query_hash(self) -> None:
        stub_query = 'stub_query'
        expected_query = 'stub_value'
        stub_registry: dict[str, Callable[[str], str]] = {stub_query: lambda *args: expected_query}

        repo = local.InMemoryQueryRepository(registry=stub_registry)

        actual_query: int = repo.get_query_hash(query_name=stub_query)

        assert actual_query == utils.hash_string(string=expected_query)

    def test_can_copy_query(self) -> None:
        source_query = 'stub_query'
        expected_query = 'stub_value'
        destination_query = 'destination_query'
        stub_registry: dict[str, Callable[[str], str]] = {
            source_query: lambda *args: expected_query
        }

        repo = local.InMemoryQueryRepository(registry=stub_registry)

        repo.copy_query(source_query_name=source_query, destination_query_name=destination_query)

        assert repo.get_query(query_name=destination_query, run_day='unused') == repo.get_query(
            query_name=source_query, run_day='unused'
        )
