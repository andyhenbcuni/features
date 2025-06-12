import time
from collections.abc import Callable, Generator, Iterable
from typing import Any

import pytest
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import dataset as bq_dataset
from google.cloud.bigquery import schema as bq_schema
from google.cloud.bigquery import table as bq_table

from src.managed_table import utils


@pytest.fixture(scope='function')
def uid() -> int:
    """
    This is required because tables are inconsistently created and deleted by BQ.
    Specifically, sometimes tables that are deleted are not fully removed
    for some unknown amount of time, causing future test runs against the table to fail
    because the fixture cannot create the table while a table of its same name still
    has existing metadata.
    """
    return abs(int(hash(str(time.time()))))


@pytest.fixture(scope='module')
def empty_table() -> Generator[Callable[..., bq_table.Table], Any, None]:
    client: bq_client.Client = utils.default_client()
    tables: list[bq_table.Table] = []

    def _create_empty_table(table_name: str, schema: list[bq_schema.SchemaField]) -> bq_table.Table:
        table_ref: bq_table.TableReference = bq_dataset.DatasetReference(
            project='nbcu-ds-algo-int-001', dataset_id='test'
        ).table(table_id=table_name)
        table: bq_table.Table = client.create_table(
            table=bq_table.Table(table_ref=table_ref, schema=schema), exists_ok=True
        )
        tables.append(table)
        return table

    yield _create_empty_table

    for table in tables:
        client.delete_table(table=table, not_found_ok=True)


@pytest.fixture(scope='class')
def populated_table() -> Generator[Callable[..., bq_table.Table], Any, None]:
    client: bq_client.Client = utils.default_client()
    tables: list[bq_table.Table] = []

    def _create_populated_table(  # noqa: PLR0913
        table_name: str,
        schema: list[bq_schema.SchemaField],
        rows: Iterable[tuple[Any, ...]],
        dataset: str | None = None,
        partition_field: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> bq_table.Table:
        # TODO: use bq repo?
        table_ref: bq_table.TableReference = bq_dataset.DatasetReference(
            project='nbcu-ds-algo-int-001', dataset_id=dataset or 'test'
        ).table(table_id=table_name)
        table_to_create = bq_table.Table(table_ref=table_ref, schema=schema)
        if labels:
            table_to_create.labels = labels

        if partition_field:
            table_to_create.time_partitioning = bq_table.TimePartitioning(
                type_=bq_table.TimePartitioningType.DAY, field=partition_field
            )
        table: bq_table.Table = client.create_table(table=table_to_create)
        tables.append(table)
        if rows:
            client.insert_rows(
                table=f'{table.dataset_id}.{table.table_id}', rows=rows, selected_fields=schema
            )
        return table

    yield _create_populated_table

    for table in tables:
        client.delete_table(table=table, not_found_ok=True)


@pytest.fixture
def test_pipe(request: pytest.FixtureRequest) -> Any:
    return request.param
