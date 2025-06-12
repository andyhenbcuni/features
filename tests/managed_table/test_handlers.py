from collections.abc import Callable, Iterable, Sequence
from contextlib import nullcontext as does_not_raise
from dataclasses import asdict
from datetime import datetime
from typing import Any

import pytest

from src.managed_table import bootstrap, utils
from src.managed_table.domain import commands, errors, events, value_objects
from src.managed_table.repositories.config.adapters import local
from src.managed_table.repositories.query.adapters.local import InMemoryQueryRepository
from src.managed_table.repositories.query.base import AbstractQueryRepository
from src.managed_table.repositories.table import exceptions
from src.managed_table.repositories.table.base import AbstractTableRepository
from src.managed_table.services import handlers, message_bus
from src.query_constructor import query_template
from tests.managed_table import helpers


class MockQueryRepository(AbstractQueryRepository):
    def __init__(self, query_map: dict[str, Any]) -> None:
        self.query_map: dict[str, Any] = query_map

    def get_query(
        self, query_name: str, run_day: str, run_time_template_fields: dict[str, Any] | None = None
    ) -> str:
        candidate = self.query_map[query_name]
        if not isinstance(candidate, Callable):
            return candidate
        else:
            return candidate(run_day)

    def copy_query(self, source_query_name: str, destination_query_name: str) -> None:
        self.query_map[destination_query_name] = self.query_map[source_query_name]

    def get_query_hash(self, query_name: str) -> int:
        return utils.hash_string(self.get_query(query_name=query_name, run_day='unused'))

    def add_query(self, query_name: str, query_renderer: Callable[[str], str]) -> None:
        self.query_map[query_name] = query_renderer


class MockTableRepository(AbstractTableRepository):
    def __init__(self, table_metadata: Iterable[value_objects.TableMetadata]) -> None:
        self.tables: dict[str, value_objects.TableMetadata] = {
            metadata.table_name: metadata for metadata in table_metadata
        }
        self._copy_table_called_with = {}
        self._write_query_results_to_table_partition_calls = []

    def __getitem__(self, key: str):
        return self.tables[key]

    def get_table_metadata(self, table_name: str) -> value_objects.TableMetadata:
        if table := self.tables.get(table_name):
            return table
        raise exceptions.TableDoesNotExistError()

    def table_exists(self, table_name: str) -> None:
        try:
            self.tables[table_name]
        except KeyError as e:
            raise exceptions.TableDoesNotExistError() from e

    def create_table(self, table_config: value_objects.TableConfig) -> None:
        self.tables[table_config.table_name] = value_objects.TableMetadata(
            **table_config.__dict__
            | {
                'created': datetime.now(),
                'updated': datetime.now(),
            }
        )

    def copy_table(
        self,
        source_table_name: str,
        destination_table_name: str,
        expires: datetime | None = None,
    ) -> None:
        self.tables[destination_table_name] = value_objects.TableMetadata(
            table_name=destination_table_name,
            schema=self.tables[source_table_name].schema,
            partition_field=self.tables[source_table_name].partition_field,
            partitions=self.tables[source_table_name].partitions,
            definition=self.tables[source_table_name].definition,
            created=self.tables[source_table_name].created,
            updated=self.tables[source_table_name].updated,
            expires=expires or self.tables[source_table_name].expires,
        )
        self._copy_table_called_with = {
            'source_table_name': source_table_name,
            'destination_table_name': destination_table_name,
        }

    def delete_table(self, table_name: str, not_found_ok: bool = False):
        if table_name in self.tables:
            del self.tables[table_name]
        elif not_found_ok:
            return

    def write_query_results_to_table_partition(self, table_name: str, query: str, partition: str):
        self.tables[table_name].partitions.append(partition)
        self._write_query_results_to_table_partition_calls.append(partition)

    def write_query_results_to_table(self, table_name: str, query: str):
        pass

    def format_definition(self, definition: str) -> str:
        return definition


def setup_dependencies(
    table_metadata: Iterable[value_objects.TableMetadata] | None = None,
    query_map: dict[str, str] | None = None,
) -> tuple[message_bus.MessageBus, MockTableRepository, MockQueryRepository]:
    table_repository = MockTableRepository(table_metadata=table_metadata or [])
    query_repository = MockQueryRepository(query_map=query_map or {})
    bus: message_bus.MessageBus = bootstrap.bootstrap(
        table_repository=table_repository, query_repository=query_repository
    )
    return bus, table_repository, query_repository


class TestCheckTableState:
    def test_returns_does_not_exist_when_table_does_not_exist(self) -> None:
        bus, _, _ = setup_dependencies()

        actual_event: message_bus.Message | Sequence[message_bus.Message] = bus.handle(
            message=commands.CheckTableExists(table_name='test_table')
        )

        assert actual_event == errors.TableDoesNotExist(table_name='test_table')


def test_update_table_updates_table():
    table_name = 'test_table'
    bus, table_repository, _ = setup_dependencies(
        table_metadata=[helpers.get_table_metadata(table_name=table_name)]
    )

    bus.handle(
        commands.UpdateTablePartition(table_name=table_name, query='unused', partition='2024-01-01')
    )

    assert '2024-01-01' in table_repository[table_name].partitions


def test_plan_backfill_produces_plan_to_update_all_partitions_in_the_specified_table_in_order():
    expected_table_metadata = helpers.get_table_metadata(
        table_name='test_table', partitions=('2024-01-01', '2024-01-02', '2024-01-03')
    )
    bus, _, _ = setup_dependencies(
        query_map={expected_table_metadata.table_name: 'SELECT 1'},
    )
    expected_commands = [
        commands.UpdateTablePartition(
            table_name=expected_table_metadata.table_name,
            query='SELECT 1',
            partition='2024-01-01',
        ),
        commands.UpdateTablePartition(
            table_name=expected_table_metadata.table_name,
            query='SELECT 1',
            partition='2024-01-02',
        ),
        commands.UpdateTablePartition(
            table_name=expected_table_metadata.table_name,
            query='SELECT 1',
            partition='2024-01-03',
        ),
    ]

    actual_commands = bus.handle(
        message=commands.PlanBackfill(
            table_name=expected_table_metadata.table_name,
            partitions=expected_table_metadata.partitions,
        ),
    )

    assert actual_commands == expected_commands


class TestCheckForNewUpstreamDependencies:
    def test_check_for_new_upstream_dependencies_returns_error_if_upstream_created_after_downstream_last_update(
        self, uid: int
    ):
        upstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
            table_name=f'upstream_table_{uid}', created=datetime(year=2024, month=1, day=2)
        )
        downstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
            table_name=f'downstream_table_{uid}', updated=datetime(year=2024, month=1, day=1)
        )

        table_repository = MockTableRepository(table_metadata=[upstream_table, downstream_table])

        cmd = commands.CheckForNewUpstreamDependencies(
            table_name=downstream_table.table_name, upstream_table_names=[upstream_table.table_name]
        )

        response = handlers.check_for_new_upstream_dependencies(
            cmd=cmd, table_repository=table_repository
        )

        assert response == errors.NewUpstreamDependenciesSinceLastUpdate(
            table_name=downstream_table.table_name
        )

    def test_check_for_new_upstream_dependencies_returns_event_if_upstream_not_created_after_downstream_last_update(
        self,
    ):
        upstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
            table_name='upstream_table', created=datetime(year=2024, month=1, day=1)
        )
        downstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
            table_name='downstream_table', updated=datetime(year=2024, month=1, day=2)
        )

        table_repository = MockTableRepository(table_metadata=[upstream_table, downstream_table])

        cmd = commands.CheckForNewUpstreamDependencies(
            table_name=downstream_table.table_name, upstream_table_names=[upstream_table.table_name]
        )

        response = handlers.check_for_new_upstream_dependencies(
            cmd=cmd, table_repository=table_repository
        )

        assert response == events.NoNewUpstreamDependencies(table_name=downstream_table.table_name)


class TestSyncPartitionedTable:
    class TestBackfillsViaSideload:
        def test_creates_backup_before_deleting_original_table(self) -> None:
            expected_table_config = helpers.get_table_config(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
            )
            existing_table_metadata = helpers.get_table_metadata(
                **asdict(expected_table_config) | {'partition_field': 'different_partition_field'}
            )
            expected_backup_name: str = f'{expected_table_config.table_name}_backup'
            bus, table_repository, query_repository = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(
                        **asdict(expected_table_config)
                        | {'partition_field': 'different_partition_field'}
                    )
                ],
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_config, query_renderer=lambda _: 'SELECT {run_day}'
            )

            bus.dispatch(message=cmd)
            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name=expected_backup_name)
            )
            assert actual_metadata
            assert actual_metadata.table_name == expected_backup_name
            assert actual_metadata.schema == existing_table_metadata.schema
            assert actual_metadata.partition_field == existing_table_metadata.partition_field
            assert actual_metadata.partitions == existing_table_metadata.partitions
            assert actual_metadata.definition == existing_table_metadata.definition

        def test_table_is_backfilled_via_sideload_if_partition_fields_do_not_match(
            self,
        ) -> None:
            expected_table_metadata: value_objects.TableMetadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
            )
            bus, table_repository, query_repository = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(
                        **asdict(expected_table_metadata)
                        | {'partition_field': 'different_partition_field'}
                    )
                ],
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert table_repository._copy_table_called_with == {
                'source_table_name': f'test_table_sideload_{query_repository.get_query_hash(query_name="test_table")}',
                'destination_table_name': 'test_table',
            }

            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

        def test_table_is_backfilled_via_sideload_if_schema_does_not_match(self):
            expected_table_metadata: value_objects.TableMetadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
                schema=[('field_1', int), ('field_2', str)],
            )
            bus, table_repository, query_repository = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(**asdict(expected_table_metadata) | {'schema': []})
                ],
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert table_repository._copy_table_called_with == {
                'source_table_name': f'test_table_sideload_{query_repository.get_query_hash(query_name="test_table")}',
                'destination_table_name': 'test_table',
            }

            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

        def test_table_is_backfilled_via_sideload_if_definition_does_not_match(self):
            expected_table_metadata: value_objects.TableMetadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
                definition='definition',
            )
            bus, table_repository, query_repository = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(
                        **asdict(expected_table_metadata) | {'definition': 'different_definition'}
                    )
                ],
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )

            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert table_repository._copy_table_called_with == {
                'source_table_name': f'test_table_sideload_{query_repository.get_query_hash(query_name="test_table")}',
                'destination_table_name': 'test_table',
            }

            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

        def test_table_is_backfilled_via_sideload_if_new_upstream_dependency(self, uid: int):
            upstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
                table_name=f'upstream_table_{uid}', created=datetime(year=2024, month=1, day=2)
            )
            downstream_table: value_objects.TableMetadata = helpers.get_table_metadata(
                table_name=f'downstream_table_{uid}', updated=datetime(year=2024, month=1, day=1)
            )
            downstream_table_config = helpers.get_table_config(
                table_name=downstream_table.table_name,
                upstream_table_names=[upstream_table.table_name],
            )

            bus, table_repository, query_repository = setup_dependencies(
                table_metadata=[upstream_table, downstream_table],
            )

            cmd = commands.SyncPartitionedTable(
                expected_metadata=downstream_table_config,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name=downstream_table.table_name)
            )
            assert table_repository._copy_table_called_with == {
                'source_table_name': f'{downstream_table.table_name}_sideload_{query_repository.get_query_hash(query_name=downstream_table.table_name)}',
                'destination_table_name': downstream_table.table_name,
            }

            assert actual_metadata
            assert actual_metadata.table_name == downstream_table_config.table_name
            assert actual_metadata.schema == downstream_table_config.schema
            assert actual_metadata.partition_field == downstream_table_config.partition_field
            assert actual_metadata.partitions == downstream_table_config.partitions
            assert actual_metadata.definition == downstream_table_config.definition

        @pytest.mark.regression
        def test_sideload_can_continue_if_sideload_table_already_exists(self):
            expected_table_metadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
                schema=[('field_1', int), ('field_2', str)],
            )

            query_repository = MockQueryRepository(
                query_map={'test_table': 'SELECT {run_day}'},
            )
            table_metadata = [
                helpers.get_table_metadata(**asdict(expected_table_metadata) | {'schema': []}),
                helpers.get_table_metadata(
                    **asdict(expected_table_metadata)
                    | {
                        'table_name': f'test_table_sideload_{query_repository.get_query_hash(query_name="test_table")}'
                    }
                ),
            ]
            table_repository = MockTableRepository(table_metadata=table_metadata)
            bus = bootstrap.bootstrap(
                table_repository=table_repository, query_repository=query_repository
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert table_repository._copy_table_called_with == {
                'source_table_name': f'test_table_sideload_{query_repository.get_query_hash(query_name="test_table")}',
                'destination_table_name': 'test_table',
            }

            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

    class TestBackfillsInPlace:
        def test_table_is_backfilled_in_place_if_table_does_not_exist(self):
            expected_table_metadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
            )
            bus, table_repository, _ = setup_dependencies(
                table_metadata=[],
            )

            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert not table_repository._copy_table_called_with
            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

        def test_table_is_backfilled_in_place_if_missing_all_partitions(self):
            expected_table_metadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
            )
            bus, table_repository, _ = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(
                        **asdict(expected_table_metadata) | {'partitions': []}
                    )
                ],
            )
            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert not table_repository._copy_table_called_with
            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

        def test_only_missing_partitions_are_backfilled_if_missing_some_partitions(self):
            expected_table_metadata = helpers.get_table_metadata(
                table_name='test_table',
                partitions=['2024-01-01', '2024-01-02', '2024-01-03'],
            )
            bus, table_repository, _ = setup_dependencies(
                table_metadata=[
                    helpers.get_table_metadata(
                        **asdict(expected_table_metadata) | {'partitions': ['2024-01-01']}
                    )
                ],
            )

            cmd = commands.SyncPartitionedTable(
                expected_metadata=expected_table_metadata,
                query_renderer=lambda _: 'SELECT {run_day}',
            )
            bus.dispatch(message=cmd)

            actual_metadata: value_objects.TableMetadata | None = (
                table_repository.get_table_metadata(table_name='test_table')
            )

            assert not table_repository._copy_table_called_with

            assert actual_metadata
            assert actual_metadata.table_name == expected_table_metadata.table_name
            assert actual_metadata.schema == expected_table_metadata.schema
            assert actual_metadata.partition_field == expected_table_metadata.partition_field
            assert actual_metadata.partitions == expected_table_metadata.partitions
            assert actual_metadata.definition == expected_table_metadata.definition

            assert table_repository._write_query_results_to_table_partition_calls == [
                '2024-01-02',
                '2024-01-03',
            ]


def test_can_add_query() -> None:
    bus, _, query_repository = setup_dependencies()
    cmd = commands.AddQuery(query_name='stub_query_name', query_renderer=lambda _: 'stub_query')

    bus.dispatch(message=cmd)

    assert (
        query_repository.get_query(query_name='stub_query_name', run_day='unused') == 'stub_query'
    )


def test_can_replace_table() -> None:
    stub_table = helpers.get_table_metadata(
        table_name='stub_table_name', definition='stub_definition'
    )
    stub_replacement_table = helpers.get_table_metadata(
        table_name='stub_replacement_table_name', definition='stub_replacement_definition'
    )
    bus, table_repo, _ = setup_dependencies(table_metadata=[stub_table, stub_replacement_table])
    cmd = commands.ReplaceTable(
        table_name=stub_table.table_name, replacement_table_name=stub_replacement_table.table_name
    )

    bus.dispatch(message=cmd)

    assert (
        table_repo.get_table_metadata(table_name=stub_table.table_name).definition
        == stub_replacement_table.definition
    )


def test_can_update_unpartitioned_table() -> None:
    stub_table: value_objects.TableMetadata = helpers.get_table_metadata()

    bus, table_repo, _ = setup_dependencies()

    cmd = commands.SyncUnpartitionedTable(table_name=stub_table.table_name, query='unused')

    with does_not_raise():  # TODO: this should be tested more explicitly
        bus.dispatch(cmd)


@pytest.mark.regression
def test_check_table_partitions_triggers_sideload_if_table_partitions_exceed_config_partitions():
    stub_table_metadata: value_objects.TableMetadata = helpers.get_table_metadata(
        partitions=['2024-01-01', '2024-01-02']
    )
    stub_table_config: value_objects.TableConfig = helpers.get_table_config(
        table_name=stub_table_metadata.table_name, partitions=['2024-01-02']
    )
    stub_table_repo = MockTableRepository(table_metadata=[stub_table_metadata])
    stub_config_repo = local.InMemoryTableConfigRepository()
    stub_config_repo.add_table_config(table_config=stub_table_config)
    cmd = commands.CheckTablePartitions(
        table_name=stub_table_config.table_name, expected_partitions=stub_table_config.partitions
    )

    message: errors.Error | events.Event = handlers.check_table_partitions(
        cmd=cmd, table_config_repository=stub_config_repo, table_repository=stub_table_repo
    )
    assert isinstance(message, errors.ExistingPartitionsExceedExpectations)
    assert message.table_name == stub_table_config.table_name


@pytest.mark.regression
def test_plan_backfill_replaces_table_name_in_query():
    stub_table_name = 'stub_table_name'
    stub_query_template = query_template.QueryTemplate(
        template='{{table_name}} {{run_day}}',
        environment_template_fields={'table_name': 'different_table_name'},
    )
    stub_query_repo = InMemoryQueryRepository(
        registry={stub_table_name: stub_query_template.render}
    )
    expected_backfill_plan: list[commands.UpdateTablePartition] = [
        commands.UpdateTablePartition(
            table_name=stub_table_name,
            query=f'{stub_table_name} 2024-01-01',
            partition='2024-01-01',
        )
    ]
    cmd = commands.PlanBackfill(table_name=stub_table_name, partitions=['2024-01-01'])

    actual_backfill_plan: Sequence[commands.Command] = handlers.plan_backfill(
        cmd=cmd, query_repository=stub_query_repo
    )

    assert actual_backfill_plan == expected_backfill_plan
