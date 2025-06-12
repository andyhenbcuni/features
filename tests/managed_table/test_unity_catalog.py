import datetime
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo

from src.managed_table.domain import value_objects
from src.managed_table.repositories.table import exceptions
from src.managed_table.repositories.table.adapters.unity_catalog import UnityCatalogTableRepository


class TestUnityCatalogTableRepository:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.mock_client = MagicMock()
        self.repo = UnityCatalogTableRepository(client=self.mock_client)

    def test_table_can_get_metadata(self):
        expected_table_name = 'test_table'
        expected_schema = [{'name': 'column_1', 'type': 'TIMESTAMP'}]
        expected_partition_field = 'column_1'
        expected_partitions = ['column_1']
        expected_definition = 'test_table_definition'
        expected_created = datetime.datetime(2024, 2, 1, 12, 0, 0)
        expected_updated = datetime.datetime(2024, 2, 2, 12, 0, 0)

        mock_table_info = MagicMock(spec=TableInfo)

        mock_table_info.full_name = expected_table_name
        mock_table_info.comment = expected_definition
        mock_table_info.partition_field = expected_partition_field
        mock_table_info.created_at = int(expected_created.timestamp() * 1000)
        mock_table_info.updated_at = int(expected_updated.timestamp() * 1000)
        mock_table_info.columns = [ColumnInfo(name='column_1', type_text='TIMESTAMP')]

        mock_partition_col = MagicMock()
        mock_partition_col.name = 'column_1'
        mock_table_info.partition_columns = [mock_partition_col]

        self.repo._get_partition_field = MagicMock(return_value=expected_partition_field)

        self.mock_client.tables.get.return_value = mock_table_info

        actual_metadata: value_objects.TableMetadata | None = self.repo.get_table_metadata(
            table_name=expected_table_name
        )

        assert actual_metadata.table_name == expected_table_name
        assert actual_metadata.schema == expected_schema
        assert actual_metadata.partition_field == expected_partition_field
        assert actual_metadata.partitions == expected_partitions
        assert actual_metadata.definition == expected_definition
        assert actual_metadata.created == expected_created
        assert actual_metadata.updated == expected_updated

    def test_get_table_metadata_table_does_not_exist(self):
        self.mock_client.tables.get.side_effect = Exception('Table not found')

        with pytest.raises(exceptions.TableDoesNotExistError):
            self.repo.get_table_metadata('nonexistent_table')

    def test_table_exists_true(self):
        self.mock_client.tables.get.return_value = MagicMock()

        self.repo.table_exists('catalog.schema.table')

    def test_table_exists_false(self):
        self.mock_client.tables.get.side_effect = Exception('Table not found')

        with pytest.raises(exceptions.TableDoesNotExistError):
            self.repo.table_exists('nonexistent_table')

    def test_create_table(self):
        table_config = value_objects.TableConfig(
            table_name='catalog.schema.table',
            schema=[{'name': 'col1', 'type': 'STRING'}],
            partition_field='col1',
            partitions=['col1'],
            definition='Test table',
        )

        self.repo.client = self.mock_client

        self.repo.create_table(table_config)

        self.mock_client.statement_execution.execute.assert_called_once_with(
            'CREATE TABLE catalog.schema.table (\n'
            '            col1 STRING\n'
            '        )\n'
            '        PARTITIONED BY (col1)\n'
            "        COMMENT 'Test table'"
        )

    def test_delete_table_success(self):
        self.repo.delete_table('catalog.schema.table')

        self.mock_client.tables.delete.assert_called_once_with('catalog.schema.table')

    def test_delete_table_not_found_ok(self):
        self.mock_client.tables.delete.side_effect = Exception('Table not found')

        self.repo.delete_table('catalog.schema.table', not_found_ok=True)

        self.mock_client.tables.delete.assert_called_once_with('catalog.schema.table')

    def test_copy_table(self):
        self.repo.copy_table('source_table', 'destination_table')

        self.mock_client.statement_execution.execute.assert_called_once_with(
            'CREATE TABLE destination_table AS SELECT * FROM source_table'
        )

    def test_write_query_results_to_table_partition(self):
        self.repo.write_query_results_to_table_partition(
            'catalog.schema.table', 'SELECT * FROM source', 'col1'
        )

        self.mock_client.statement_execution.execute.assert_called_once_with(
            'INSERT INTO catalog.schema.table PARTITION (col1) SELECT * FROM source'
        )

    def test_write_query_results_to_table(self):
        self.repo.write_query_results_to_table('catalog.schema.table', 'SELECT * FROM source')

        self.mock_client.statement_execution.execute.assert_called_once_with(
            'INSERT INTO catalog.schema.table SELECT * FROM source'
        )
