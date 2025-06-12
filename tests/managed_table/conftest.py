import time
from collections.abc import Callable, Collection, Generator, Iterable
from typing import Any

import pytest
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import dataset as bq_dataset
from google.cloud.bigquery import schema as bq_schema
from google.cloud.bigquery import table as bq_table

from src.common import environment  # TODO: mv this to bootstrap?


@pytest.fixture(scope='function')
def uid():
    """
    This is required because tables are inconsistently created and deleted by BQ.
    Specifically, sometimes tables that are deleted are not fully removed
    for some unknown amount of time, causing future test runs against the table to fail
    because the fixture cannot create the table while a table of its same name still
    has existing metadata.
    """
    return abs(int(hash(str(time.time()))))


@pytest.fixture(scope='function')
def empty_table() -> Generator[Callable[..., bq_table.Table], Any, None]:
    client: bq_client.Client = bq_client.Client(project=environment.BQ_BILLING_PROJECT)
    tables: list[bq_table.Table] = []

    def _create_empty_table(table_name: str, schema: Collection[tuple[str, str]]) -> bq_table.Table:
        schema_fields = [
            bq_schema.SchemaField(name=name, field_type=field_type) for name, field_type in schema
        ]
        table_ref: bq_table.TableReference = bq_dataset.DatasetReference(
            project='nbcu-ds-algo-int-001',
            dataset_id='algo_features',  # TODO: inject
        ).table(table_id=table_name)

        table = bq_table.Table(table_ref=table_ref, schema=schema_fields)
        tables.append(client.create_table(table=table, exists_ok=True))
        return table

    yield _create_empty_table

    for table in tables:
        client.delete_table(table=table, not_found_ok=True)


@pytest.fixture(scope='function')
def empty_partitioned_table() -> Generator[Callable[..., bq_table.Table], Any, None]:
    client: bq_client.Client = bq_client.Client(project=environment.BQ_BILLING_PROJECT)
    tables: list[bq_table.Table] = []

    def _create_empty_table(
        table_name: str,
        schema: Collection[tuple[str, str]],
        partition_field: str,
        labels: dict[str, str] | None = None,
    ) -> bq_table.Table:
        schema_fields = [
            bq_schema.SchemaField(name=name, field_type=field_type) for name, field_type in schema
        ]
        table_ref: bq_table.TableReference = bq_dataset.DatasetReference(
            project='nbcu-ds-algo-int-001',
            dataset_id='algo_features',  # TODO: inject
        ).table(table_id=table_name)

        table = bq_table.Table(table_ref=table_ref, schema=schema_fields)
        table.time_partitioning = bq_table.TimePartitioning(
            type_=bq_table.TimePartitioningType.DAY, field=partition_field
        )
        table.labels = labels or {}
        tables.append(client.create_table(table=table, exists_ok=True))
        return table

    yield _create_empty_table

    for table in tables:
        client.delete_table(table=table, not_found_ok=True)


@pytest.fixture(scope='class')
def populated_table() -> Generator[Callable[..., bq_table.Table], Any, None]:
    client: bq_client.Client = bq_client.Client(project=environment.BQ_BILLING_PROJECT)
    tables: list[bq_table.Table] = []

    def _create_populated_table(
        table_name: str, schema: list[bq_schema.SchemaField], rows: Iterable[tuple[Any, ...]]
    ) -> bq_table.Table:
        table_ref: bq_table.TableReference = bq_dataset.DatasetReference(
            project='nbcu-ds-algo-int-001', dataset_id='test'
        ).table(table_id=table_name)
        table: bq_table.Table = client.create_table(
            table=bq_table.Table(table_ref=table_ref, schema=schema)
        )
        tables.append(table)
        client.insert_rows(
            table=f'{table.dataset_id}.{table.table_id}', rows=rows, selected_fields=schema
        )
        return table

    yield _create_populated_table

    for table in tables:
        client.delete_table(table=table, not_found_ok=True)
