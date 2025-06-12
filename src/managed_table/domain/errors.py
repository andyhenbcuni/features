from collections.abc import Sequence
from dataclasses import dataclass

from src.managed_table.domain import value_objects
from src.managed_table.domain.messages import Message


class Error(Message): ...


@dataclass
class TableDoesNotExist(Error):
    table_name: str


@dataclass
class TableAlreadyExists(Error):
    table_metadata: value_objects.TableMetadata


@dataclass
class TableHasNoPartitions(Error):
    table_name: str
    missing_partitions: Sequence[str]


@dataclass
class PartitionFieldDoesNotMatchExpectation(Error):
    table_name: str


@dataclass
class SchemaDoesNotMatchExpectation(Error):
    table_name: str


@dataclass
class DefinitionDoesNotMatchExpectation(Error):
    table_name: str


@dataclass
class PartitionsDoNotMatchExpectation(Error):
    table_name: str
    missing_partitions: Sequence[str]


@dataclass
class ExistingPartitionsExceedExpectations(Error):
    table_name: str


@dataclass
class NewUpstreamDependenciesSinceLastUpdate(Error):
    table_name: str
