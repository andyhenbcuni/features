from typing import Any

from google.cloud.bigquery import client as bq_client
from google.cloud.bigquery import job

from src.common import environment


def default_client() -> bq_client.Client:
    return bq_client.Client(project=environment.BQ_BILLING_PROJECT)


def get_bq_schema_api_repr_from_query_dry_run(
    query: str,
) -> list[dict[str, Any]]:
    client: bq_client.Client = default_client()
    query_job: job.QueryJob = client.query(
        query=query,
        job_config=job.QueryJobConfig(dry_run=True),
    )
    # TODO: this feels dangerous, but is currently the only way to pull this information without running a query
    return query_job._properties['statistics']['query']['schema']['fields']  # pyright: ignore[reportAttributeAccessIssue]
