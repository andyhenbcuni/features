import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import enums
from google.cloud.bigquery import job as bq_job
from google.cloud.bigquery import schema as bq_schema
from google.cloud.bigquery import table as bq_table

from src.common import environment
from src.managed_table import utils
from src.managed_table.domain import value_objects
from src.managed_table.repositories.table import base, exceptions

logger = utils.get_logger(name='bigquery_table_repository')


def default_client() -> bq_client.Client:
    return bq_client.Client(project=environment.BQ_BILLING_PROJECT)


class QueryReturnedNoDataError(Exception):
    pass


@dataclass
class BigQueryTableRepository(base.AbstractTableRepository):
    client: bq_client.Client = field(default_factory=default_client)

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
        except google_exceptions.NotFound as e:
            msg: str = f'Table: {table_name} does not exist.'
            raise exceptions.TableDoesNotExistError(msg) from e

    def _get_definition(self, table_name: str) -> str:
        if table := self._get_table(table_name=table_name):
            return table.labels.get('definition', '')
        return ''

    def _get_schema(self, table_name: str) -> list[dict[str, Any]]:
        if table := self._get_table(table_name=table_name):
            return [field.to_api_repr() for field in table.schema]
        return []

    def _get_partition_field(self, table_name: str) -> str:
        if table := self._get_table(table_name=table_name):
            if (table.time_partitioning is not None) and (
                table.time_partitioning.field is not None
            ):
                return table.time_partitioning.field
        return ''

    def _get_creation_time(self, table_name: str) -> datetime.datetime:
        table: bq_table.Table = self._get_table(table_name=table_name)
        if table.created is None:
            msg = f'Table: {table_name} has no creation timestamp.'
            raise NotImplementedError(msg)
        return table.created

    def _get_last_update_time(self, table_name: str) -> datetime.datetime:
        table: bq_table.Table = self._get_table(table_name=table_name)
        if table.modified is None:
            msg = f'Table: {table_name} has timestamp for its last update.'
            raise NotImplementedError(msg)
        return table.modified

    def _get_table(self, table_name: str) -> bq_table.Table:
        table_id: str = self._convert_table_name_to_id(table_name=table_name)
        return self.client.get_table(table=table_id)

    def table_exists(self, table_name: str) -> None:
        try:
            _: bq_table.Table = self._get_table(table_name=table_name)
        except google_exceptions.NotFound as e:
            msg: str = f'Table: {table_name} does not exist.'
            raise exceptions.TableDoesNotExistError(msg) from e

    def _get_partitions(self, table_name: str) -> list[str]:
        project, dataset, table = self._convert_table_name_to_id(table_name=table_name).split('.')

        job: bq_job.QueryJob = self.client.query(
            query=utils.read_template(
                Path(__file__).parent / 'templates' / 'get_partitions.sql.jinja2',
                template_fields={
                    'project': project,
                    'dataset': dataset,
                    'table': table,
                },
            )
        )
        return sorted([row['days'].strftime('%Y-%m-%d') for row in job.result()])

    def create_table(self, table_config: value_objects.TableConfig) -> None:
        schema_fields: list[bq_schema.SchemaField] = self._convert_schema_to_schema_fields(
            schema=table_config.schema
        )
        table = bq_table.Table(
            table_ref=self._convert_table_name_to_id(table_name=table_config.table_name),
            schema=schema_fields,
        )
        if table_config.expires:
            table.expires = table_config.expires
        table.time_partitioning = bq_table.TimePartitioning(
            type_=bq_table.TimePartitioningType.DAY, field=table_config.partition_field
        )
        table.labels = {'definition': self.format_definition(definition=table_config.definition)}
        self.client.create_table(table=table)

    def _convert_schema_to_schema_fields(self, schema):
        return [bq_schema.SchemaField.from_api_repr(field) for field in schema]

    def copy_table(
        self,
        source_table_name: str,
        destination_table_name: str,
        expires: datetime.datetime | None = None,
    ) -> None:
        logger.info(
            f'Copying source table: {source_table_name} to destination table: {destination_table_name}'
        )
        job_config = bq_job.CopyJobConfig()
        if expires:
            job_config.destination_expiration_time = expires  # type: ignore[reportAttributeAccessIssue]

        job: bq_job.CopyJob = self.client.copy_table(
            sources=self._convert_table_name_to_id(table_name=source_table_name),
            destination=self._convert_table_name_to_id(table_name=destination_table_name),
            job_config=job_config,
        )
        logger.info(f'Job id: {job.job_id}. Link: {job.self_link}')
        job.result()
        if job.error_result:
            raise Exception(f'Copy job failed. Job metadata: {job}')
        logger.info('Copy complete.')

    def delete_table(self, table_name: str, not_found_ok: bool = False) -> None:
        self.client.delete_table(
            table=self._convert_table_name_to_id(table_name=table_name), not_found_ok=not_found_ok
        )

    def write_query_results_to_table_partition(self, table_name: str, query: str, partition: str):
        destination: str = '$'.join(
            [
                self._convert_table_name_to_id(table_name=table_name),
                partition.replace('-', ''),
            ]
        )

        job = self.client.query(
            query=query,
            job_config=self._get_query_job_config(destination=destination),
        )
        logger.info(f'Writing query results to table. Job id: {job.job_id}. Link: {job.self_link}.')
        result = job.result()
        if result.total_rows == 0:
            msg = f'Attempted to write to partition: {partition} in table: {table_name}, but the query returned no data.\n Query: {query}'
            raise QueryReturnedNoDataError(msg)

    def write_query_results_to_table(self, table_name: str, query: str):
        self.client.query(
            query=query,
            job_config=self._get_query_job_config(
                destination=self._convert_table_name_to_id(table_name=table_name)
            ),
        ).result()

    # TODO: format_definitionegression test to ensure definitions are compared
    def format_definition(self, definition: str) -> str:
        return str(utils.hash_string(string=definition))[
            :63
        ]  # bq labels have to be < 63 characters

    @staticmethod
    def _convert_table_name_to_id(table_name: str) -> str:
        return environment.BQ_DESTINATION.format(table_id=table_name)

    def _get_query_job_config(
        self,
        destination: str,
        dry_run: bool | None = None,
    ) -> bq_job.QueryJobConfig:
        return bq_job.QueryJobConfig(
            destination=destination,
            priority=enums.QueryPriority.INTERACTIVE,
            dry_run=dry_run,
            write_disposition=bq_job.WriteDisposition.WRITE_TRUNCATE,
            use_query_cache=False,
            use_legacy_sql=False,
        )
