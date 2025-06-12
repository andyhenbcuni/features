import argparse
import inspect
import json
import os
import pathlib
import time
from datetime import datetime
from logging import Logger
from typing import Any

import pandas as pd
import yaml
from google.api_core import exceptions as google_exceptions
from google.api_core.page_iterator import Iterator
from google.cloud.bigquery import client as bq_client

from src.actions import utils as bq_utils
from src.common import environment, paths
from src.managed_table.entrypoints import local
from src.query_constructor import query_template
from src.scripts import utils

LOGGER: Logger = utils.get_logger(name='actions')


def default_environment_template_fields(start_date: str, table_name: str) -> dict[str, str]:
    return {
        'algo_project': os.environ.get('AIRFLOW_VAR_ALGO_PROJECT', default='nbcu-ds-algo-int-001'),
        'data_env': 'nbcu-ds-prod-001',
        'dataset': 'algo_features',
        'start_date': start_date,
        'table_name': table_name,
    }


def restore_table_from_backup(
    backup_table_name: str,
    destination_table_name: str,
    managed_table_entrypoint: local.Entrypoint | None = None,
) -> None:
    if managed_table_entrypoint is None:
        managed_table_entrypoint = local.Entrypoint.from_default()

    managed_table_entrypoint.replace_table(
        table_name=backup_table_name, replacement_table_name=destination_table_name
    )


def sync_partitioned_table(  # noqa: PLR0913
    table_name: str,
    start_date: str,
    run_day: str,
    partition_field: str,
    upstream_table_names: list[str],
    managed_table_entrypoint: local.Entrypoint | None = None,
) -> None:
    if managed_table_entrypoint is None:
        managed_table_entrypoint = local.Entrypoint.from_default()

    query: query_template.QueryTemplate = get_query(
        query_name=table_name,
        environment_template_fields=default_environment_template_fields(
            start_date=start_date, table_name=table_name
        ),
    )

    managed_table_entrypoint.sync_partitioned_table(
        table_name=table_name,
        schema=bq_utils.get_bq_schema_api_repr_from_query_dry_run(
            query=query.render(run_day=start_date)
        ),  # TODO: this is extremely fragile for recursive queries, must be start date
        partition_field=partition_field,
        upstream_table_names=upstream_table_names,
        partitions=[
            datetime.strftime(day, '%Y-%m-%d')
            for day in pd.date_range(start=start_date, end=run_day)
        ],
        definition=query.render(run_day='unused'),
        query_renderer=query.render,
    )


def sync_unpartitioned_table(
    table_name: str,
    run_day: str,
    managed_table_entrypoint: local.Entrypoint | None = None,
) -> None:
    if managed_table_entrypoint is None:
        managed_table_entrypoint = local.Entrypoint.from_default()

    query: query_template.QueryTemplate = get_query(
        query_name=table_name,
        environment_template_fields=default_environment_template_fields(
            start_date='unused', table_name=table_name
        ),
    )

    managed_table_entrypoint.sync_unpartitioned_table(
        table_name=table_name,
        query=query.render(run_day=run_day),
    )


def get_query(
    query_name: str, environment_template_fields: dict[str, Any]
) -> query_template.QueryTemplate:
    query_path: pathlib.Path = _get_query_candidate(query_name=query_name)
    return get_query_template(
        query_path=query_path, environment_template_fields=environment_template_fields
    )


def get_query_template(
    query_path: pathlib.Path, environment_template_fields: dict[str, Any]
) -> query_template.QueryTemplate:
    with query_path.open() as f:
        query: str = f.read()
    match query_path.suffix:
        case '.jinja2':
            return query_template.QueryTemplate(
                template=query, environment_template_fields=environment_template_fields
            )
        case '.yaml':
            config: dict[str, Any] = yaml.safe_load(stream=query)
            # resolving base query for recursive templates
            if config['template'] == 'recursive_template':
                base_query_path: pathlib.Path = (
                    query_path.parent / config['template_fields']['query']
                )
                config['template_fields']['query'] = _get_base_query(query_path=base_query_path)
            return query_template.QueryTemplate.from_registry(
                name=config['template'],
                user_defined_template_fields=config['template_fields'],
                environment_template_fields=environment_template_fields,
            )
        case _:
            msg: str = f'Only templates of type .jinja2 and .yaml are supported. Received {query_path.suffix}'
            raise NotImplementedError(msg)


def _get_base_query(query_path: pathlib.Path) -> str:
    with query_path.open() as f:
        query: str = f.read()
    match query_path.suffix:
        case '.jinja2':
            return query
        case _:
            msg: str = f'Only templates of type .jinja2 are supported. Received {query_path.suffix}'
            raise NotImplementedError(msg)


def _get_query_candidate(query_name: str) -> pathlib.Path:
    candidates = list(paths.get_path(path_type='configs').rglob(pattern=f'{query_name}.*'))

    if len(candidates) > 1:
        sql_candidates: list[pathlib.Path] = [
            candidate for candidate in candidates if 'sql' in candidate.name
        ]
        if len(sql_candidates) == 1:
            return sql_candidates[0]
        raise ValueError(f'Multiple candidates found for query name: {query_name}')
    if not candidates:
        msg: str = f'Candidate not found for query: {query_name}.'
        raise NotImplementedError(msg)
    candidate: pathlib.Path = candidates[0]
    return candidate


def run_bq_assertion(
    assertion: str,
    run_day: str,
    template_fields: dict[str, Any] | None = None,
    client: bq_client.Client | None = None,
) -> None:
    if template_fields is None:
        template_fields = {}
    if client is None:
        client = bq_utils.default_client()
    query: query_template.QueryTemplate = get_query(
        query_name=assertion,
        environment_template_fields=default_environment_template_fields(
            start_date='unused', table_name='unused'
        )
        | template_fields,
    )
    job = client.query(query=query.render(run_day=run_day))

    try:
        job.result()
    except google_exceptions.BadRequest as e:
        msg = 'Assertion failed'
        raise ValueError(msg) from e


def check_bq_partition(
    table_name: str, dataset: str, run_day: str, retry: int = 12, retry_delay: int = 300
) -> None:
    while retry > 0:
        try:
            return run_bq_assertion(
                assertion='assert_partition_exists',
                run_day=run_day,
                template_fields={
                    'table': table_name,
                    'dataset': dataset,
                    'project': environment.BQ_DATA_ENV,
                },
            )
        except ValueError:
            time.sleep(retry_delay)
            retry -= 1
    msg = 'Assertion retry limit reached.'
    raise ValueError(msg)


def cleanup_sideload_tables_in_bigquery(client: bq_client.Client | None = None):
    if client is None:
        client = bq_utils.default_client()
    tables: Iterator = client.list_tables(dataset=environment.BQ_DATASET)

    for table in tables:
        if 'sideload' in table.table_id:
            LOGGER.info(f'Deleting table {table.table_id} in dataset: {environment.BQ_DATASET}')
            client.delete_table(table=table, not_found_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description='Run an action.')
    parser.add_argument('action', type=str, help='The name of the action to run.')
    parser.add_argument(
        '--parameters',
        help='Parameters in JSON format.',
        type=json.loads,
        required=False,
        default={},
    )
    parser.add_argument(
        '--run-time-parameters',
        help='Run time in JSON format.',
        type=json.loads,
        required=False,
        default={},
    )

    args: argparse.Namespace = parser.parse_args()

    action = globals().get(args.action)
    if not callable(action):
        msg: str = f"Function '{args.action}' does not exist."
        raise ValueError(msg)

    all_parameters = args.parameters | args.run_time_parameters

    # TODO: this could be refined.
    # filter out unused runtime parameters
    filtered_parameters = {
        parameter: all_parameters[parameter]
        for parameter in inspect.signature(action).parameters
        if parameter in all_parameters
    }

    action(**filtered_parameters)


if __name__ == '__main__':
    main()
