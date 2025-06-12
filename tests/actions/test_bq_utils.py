import pytest
from google.cloud.bigquery import schema

from src.actions import utils


@pytest.mark.network
def test_get_schema_from_query_dry_run_can_handle_repeated_fields() -> None:
    query = "SELECT 'a' AS a, ARRAY((SELECT AS STRUCT 'b' AS b, 'c'AS c, ARRAY((SELECT 'd' AS d UNION ALL SELECT 'D')) AS d_array )) AS b_array"
    expected_schema: list[schema.SchemaField] = [
        schema.SchemaField(name='a', field_type='STRING', mode='NULLABLE'),
        schema.SchemaField(
            name='b_array',
            field_type='RECORD',
            mode='REPEATED',
            fields=[
                schema.SchemaField(name='b', field_type='STRING', mode='NULLABLE'),
                schema.SchemaField(name='c', field_type='STRING', mode='NULLABLE'),
                schema.SchemaField(name='d_array', field_type='STRING', mode='REPEATED'),
            ],
        ),
    ]

    actual_schema: list[schema.SchemaField] = utils.get_schema_from_query_dry_run(
        client=utils.default_client(), query=query
    )

    assert actual_schema == expected_schema
