from collections.abc import Collection
from dataclasses import dataclass, field
from typing import Any

from src.managed_table.domain.messages import Message


class Event(Message): ...


@dataclass(frozen=True)
class TableExists(Event):
    table_name: str


@dataclass(frozen=True)
class TableUpToDate(Event):
    table_name: str


@dataclass(frozen=True)
class TableDeleted(Event):
    table_name: str


@dataclass(frozen=True)
class TableCopied(Event):
    source_table_name: str
    destination_table_name: str


@dataclass(frozen=True)
class TableCreated(Event):
    table_name: str
    schema: Collection[dict[str, Any]] = field(repr=False)
    partition_field: str


@dataclass(frozen=True)
class TablePartitionUpdated(Event):
    table_name: str
    query: str
    partition: str


@dataclass(frozen=True)
class TableDefinitionUpToDate(Event):
    table_name: str


@dataclass(frozen=True)
class TablePartitionFieldUpdateToDate(Event):
    table_name: str


@dataclass(frozen=True)
class TableSchemaUpToDate(Event):
    table_name: str


@dataclass(frozen=True)
class TablePartitionsExist(Event):
    table_name: str


@dataclass(frozen=True)
class TablePartitionsUpToDate(Event):
    table_name: str


@dataclass(frozen=True)
class NoNewUpstreamDependencies(Event):
    table_name: str


@dataclass(frozen=True)
class QueryAdded(Event):
    query_name: str


@dataclass(frozen=True)
class TableReplaced(Event):
    table_name: str


@dataclass(frozen=True)
class TableSynchronized(Event):
    table_name: str
