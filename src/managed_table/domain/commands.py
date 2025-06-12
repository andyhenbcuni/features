from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.managed_table.domain import value_objects
from src.managed_table.domain.messages import Message


class Command(Message): ...


@dataclass(frozen=True)
class CheckTableState(Command):
    expected_metadata: value_objects.TableConfig


@dataclass(frozen=True)
class CheckTableExists(Command):
    table_name: str


@dataclass(frozen=True)
class CheckForNewUpstreamDependencies(Command):
    table_name: str
    upstream_table_names: list[str]


@dataclass(frozen=True)
class CheckTableDefinition(Command):
    table_name: str


@dataclass(frozen=True)
class CheckTablePartitionField(Command):
    table_name: str


@dataclass(frozen=True)
class CheckTableSchema(Command):
    table_name: str
    expected_schema: Collection[dict[str, Any]]


@dataclass(frozen=True)
class CheckTablePartitionsAreNotEmpty(Command):
    table_name: str


@dataclass(frozen=True)
class CheckTablePartitions(Command):
    table_name: str
    expected_partitions: Sequence[str]


@dataclass(frozen=True)
class UpdateTablePartition(Command):
    table_name: str
    query: str
    partition: str


@dataclass(frozen=True)
class CreateTable(Command):
    table_name: str


@dataclass(frozen=True)
class CopyTable(Command):
    source_table_name: str
    destination_table_name: str
    expires: datetime | None = None


@dataclass(frozen=True)
class DeleteTable(Command):
    table_name: str
    not_found_ok: bool = False


@dataclass(frozen=True)
class PlanBackfill(Command):
    table_name: str
    partitions: Sequence[str]


@dataclass(frozen=True)
class PlanSideload(Command):
    table_name: str


@dataclass(frozen=True)
class AddQuery(Command):
    query_name: str
    query_renderer: Callable[[str, dict | None], str]


@dataclass(frozen=True)
class ReplaceTable(Command):
    table_name: str
    replacement_table_name: str


@dataclass(frozen=True)
class SyncPartitionedTable(Command):
    expected_metadata: value_objects.TableConfig
    query_renderer: Callable[[str, dict | None], str]


@dataclass(frozen=True)
class SyncUnpartitionedTable(Command):
    table_name: str
    query: str
