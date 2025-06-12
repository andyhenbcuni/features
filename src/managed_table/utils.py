import hashlib
import logging
from pathlib import Path
from typing import Any, NoReturn

import jinja2
from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import job, schema

from src.common import environment


class TemplateException(Exception):
    pass


def raise_template_exception(message: str) -> NoReturn:
    raise TemplateException(message)


jinja2_environment = jinja2.environment.Environment()
jinja2_environment.globals['raise_template_exception'] = raise_template_exception


def read_template(template_path: Path, template_fields: dict[str, Any] | None = None) -> str:
    """Function for reading and formatting a jinja template.

    Args:
        template_path (list[str]): path to the template
        template_fields (Optional[dict], optional): fields to template. Defaults to None.

    Returns:
        (str): formatted template
    """

    with template_path.open() as file:
        template: jinja2.Template = jinja2_environment.from_string(source=file.read())
        rendered: str = template.render(**(template_fields or {}))
    return rendered


def default_client() -> bq_client.Client:
    return bq_client.Client(project=environment.BQ_BILLING_PROJECT)


def get_schema_from_query_dry_run(
    query: str,
) -> list[schema.SchemaField]:
    client = default_client()
    query_job: job.QueryJob = client.query(
        query=query,
        job_config=job.QueryJobConfig(dry_run=True),
    )
    # TODO: this feels dangerous, but is currently the only way to pull this information without running a query
    return [
        schema.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'])
        for field in query_job._properties['statistics']['query']['schema']['fields']  # pyright: ignore[reportAttributeAccessIssue]
    ]


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.hasHandlers():
        # define formatter
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')

        # define file handler
        file_handler = logging.FileHandler(
            f'{name}.log',
            encoding='utf-8',
            mode='w',
        )
        log_handler(file_handler, formatter, logger)
        # define stream handler
        stream_handler = logging.StreamHandler()
        log_handler(stream_handler, formatter, logger)
    return logger


def log_handler(handler, formatter, logger):
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


logger: logging.Logger = get_logger('utils')


def hash_string(string: str) -> int:
    return abs(
        int(
            hashlib.sha256(string=string.encode(encoding='utf-8')).hexdigest(),
            16,
        )
    )
