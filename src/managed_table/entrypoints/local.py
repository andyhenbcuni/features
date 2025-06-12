from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from src.managed_table import bootstrap, configs
from src.managed_table.domain import commands, value_objects
from src.managed_table.services import message_bus


@dataclass
class Entrypoint:
    bus: message_bus.MessageBus

    def sync_partitioned_table(  # noqa: PLR0913
        self,
        table_name: str,
        schema: Collection[dict[str, Any]],
        partition_field: str,
        partitions: Sequence[str],
        definition: str,
        upstream_table_names: list[str],
        query_renderer: Callable[[str, dict | None], str],
    ) -> None:
        expected_metadata = value_objects.TableConfig(
            table_name=table_name,
            schema=schema,
            partition_field=partition_field,
            partitions=partitions,
            definition=definition,
            upstream_table_names=upstream_table_names,
        )
        cmd = commands.SyncPartitionedTable(
            expected_metadata=expected_metadata, query_renderer=query_renderer
        )
        self.bus.dispatch(message=cmd)

    def sync_unpartitioned_table(self, table_name: str, query: str) -> None:
        cmd = commands.SyncUnpartitionedTable(table_name=table_name, query=query)
        self.bus.dispatch(message=cmd)

    def add_query(self, query_name: str, query_renderer: Callable[[str, dict | None], str]) -> None:
        cmd = commands.AddQuery(query_name=query_name, query_renderer=query_renderer)
        self.bus.dispatch(message=cmd)

    def replace_table(self, table_name: str, replacement_table_name: str) -> None:
        cmd = commands.ReplaceTable(
            table_name=table_name, replacement_table_name=replacement_table_name
        )
        self.bus.dispatch(message=cmd)

    @classmethod
    def from_config(cls, config: configs.BootstrapConfig) -> Self:
        return cls(
            bus=bootstrap.bootstrap_from_config(config=config),
        )

    @classmethod
    def from_default(cls) -> Self:
        return cls(
            bus=bootstrap.bootstrap_from_config(config=configs.DEFAULT_CONFIG),
        )
