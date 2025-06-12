from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class TableConfig:
    """User defined configuration of a table."""

    table_name: str
    schema: Collection[dict[str, Any]] = field(repr=False)
    partition_field: str
    partitions: Sequence[str]
    definition: str
    upstream_table_names: list[str] = field(default_factory=list)
    expires: datetime | None = None


@dataclass(frozen=True, kw_only=True)
class TableMetadata(TableConfig):
    """Actual metadata of an existing table."""

    created: datetime
    updated: datetime
