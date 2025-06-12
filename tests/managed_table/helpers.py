from collections.abc import Collection, Sequence
from datetime import datetime

from src.managed_table.domain import value_objects


def get_table_config(  # noqa: PLR0913
    table_name: str = 'unused',
    schema: Collection[tuple[str, str]] = [('unused', 'unused')],
    partition_field: str = 'unused',
    partitions: Sequence[str] = [],
    definition: str = '',
    upstream_table_names: list[str] = [],
    expires: datetime | None = None,
) -> value_objects.TableConfig:
    return value_objects.TableConfig(
        table_name=table_name,
        schema=schema,
        partition_field=partition_field,
        partitions=partitions,
        definition=definition,
        upstream_table_names=upstream_table_names,
        expires=expires,
    )


def get_table_metadata(  # noqa: PLR0913
    table_name: str = 'unused',
    schema: Collection[tuple[str, str]] = [('unused', 'unused')],
    partition_field: str = 'unused',
    partitions: Sequence[str] = [],
    definition: str = '',
    created: datetime = datetime(year=2024, month=1, day=1),
    updated: datetime = datetime(year=2024, month=1, day=1),
    upstream_table_names: list[str] = [],
    expires: datetime | None = None,
) -> value_objects.TableMetadata:
    return value_objects.TableMetadata(
        table_name=table_name,
        schema=schema,
        partition_field=partition_field,
        partitions=partitions,
        definition=definition,
        created=created,
        updated=updated,
        upstream_table_names=upstream_table_names,
        expires=expires,
    )
