from collections.abc import Callable, MutableSequence, Sequence
from datetime import datetime
from typing import Any

from src.managed_table.domain import commands, errors, events, value_objects
from src.managed_table.repositories.config.base import AbstractTableConfigRepository
from src.managed_table.repositories.query.base import AbstractQueryRepository
from src.managed_table.repositories.table import exceptions
from src.managed_table.repositories.table.base import AbstractTableRepository


def trigger_table_creation(
    error: errors.TableDoesNotExist,
) -> commands.CreateTable:
    return commands.CreateTable(table_name=error.table_name)


def trigger_backfill_plan(
    error: errors.TableHasNoPartitions | errors.PartitionsDoNotMatchExpectation,
) -> commands.PlanBackfill:
    return commands.PlanBackfill(
        table_name=error.table_name,
        partitions=error.missing_partitions,
    )


def trigger_sideload_plan(
    error: (errors.PartitionFieldDoesNotMatchExpectation | errors.SchemaDoesNotMatchExpectation),
) -> commands.PlanSideload:
    return commands.PlanSideload(table_name=error.table_name)


def check_table_exists(
    cmd: commands.CheckTableExists, table_repository: AbstractTableRepository
) -> events.TableExists | errors.TableDoesNotExist:
    try:
        table_repository.table_exists(table_name=cmd.table_name)
    except exceptions.TableDoesNotExistError:
        return errors.TableDoesNotExist(table_name=cmd.table_name)
    return events.TableExists(table_name=cmd.table_name)


def check_table_definition(
    cmd: commands.CheckTableDefinition,
    table_config_repository: AbstractTableConfigRepository,
    table_repository: AbstractTableRepository,
) -> errors.Error | events.Event:
    formatted_definition = table_repository.format_definition(
        definition=table_config_repository.get_table_config(table_name=cmd.table_name).definition
    )
    if (
        table_repository.get_table_metadata(table_name=cmd.table_name).definition
        != formatted_definition
    ):
        return errors.DefinitionDoesNotMatchExpectation(table_name=cmd.table_name)
    return events.TableDefinitionUpToDate(table_name=cmd.table_name)


def check_table_partition_field(
    cmd: commands.CheckTablePartitionField,
    table_config_repository: AbstractTableConfigRepository,
    table_repository: AbstractTableRepository,
) -> errors.Error | events.Event:
    if (
        table_repository.get_table_metadata(table_name=cmd.table_name).partition_field
        != table_config_repository.get_table_config(table_name=cmd.table_name).partition_field
    ):
        return errors.PartitionFieldDoesNotMatchExpectation(table_name=cmd.table_name)
    return events.TablePartitionFieldUpdateToDate(table_name=cmd.table_name)


def check_table_schema(
    cmd: commands.CheckTableSchema,
    table_config_repository: AbstractTableConfigRepository,
    table_repository: AbstractTableRepository,
) -> errors.Error | events.Event:
    if (
        table_repository.get_table_metadata(table_name=cmd.table_name).schema
        != table_config_repository.get_table_config(table_name=cmd.table_name).schema
    ):
        return errors.SchemaDoesNotMatchExpectation(table_name=cmd.table_name)
    return events.TableSchemaUpToDate(table_name=cmd.table_name)


def check_table_partitions_are_not_empty(
    cmd: commands.CheckTablePartitionsAreNotEmpty,
    table_config_repository: AbstractTableConfigRepository,
    table_repository: AbstractTableRepository,
) -> errors.Error | events.Event:
    table_config: value_objects.TableConfig = table_config_repository.get_table_config(
        table_name=cmd.table_name
    )

    if (
        not table_repository.get_table_metadata(table_name=cmd.table_name).partitions
        and table_config.partitions
    ):
        return errors.TableHasNoPartitions(
            table_name=cmd.table_name, missing_partitions=table_config.partitions
        )  # TODO: deprecate missing partitions
    return events.TablePartitionsExist(table_name=cmd.table_name)


def check_table_partitions(
    cmd: commands.CheckTablePartitions,
    table_config_repository: AbstractTableConfigRepository,
    table_repository: AbstractTableRepository,
) -> errors.Error | events.Event:
    table_partitions: Sequence[str] = table_repository.get_table_metadata(
        table_name=cmd.table_name
    ).partitions
    table_config_partitions: Sequence[str] = table_config_repository.get_table_config(
        table_name=cmd.table_name
    ).partitions

    if table_config_partitions:
        config_start_date = datetime.strptime(min(table_config_partitions), '%Y-%m-%d')

        past_excess_partitions = [
            partition
            for partition in table_partitions
            if datetime.strptime(partition, '%Y-%m-%d') < config_start_date
        ]

        if past_excess_partitions:
            return errors.ExistingPartitionsExceedExpectations(table_name=cmd.table_name)
    if table_partitions != table_config_partitions:
        return errors.PartitionsDoNotMatchExpectation(
            table_name=cmd.table_name,
            missing_partitions=[
                partition
                for partition in table_config_partitions
                if partition not in table_partitions
            ],
        )
    return events.TablePartitionsUpToDate(table_name=cmd.table_name)


# TODO: add logic for backfilling recursive tables, e.g. if missing partitions are discontinuous
def check_table_state(
    cmd: commands.CheckTableState, table_config_repository: AbstractTableConfigRepository
) -> Sequence[commands.Command | events.Event]:
    table_config_repository.add_table_config(table_config=cmd.expected_metadata)
    return [
        commands.CheckTableExists(table_name=cmd.expected_metadata.table_name),
        commands.CheckForNewUpstreamDependencies(
            table_name=cmd.expected_metadata.table_name,
            upstream_table_names=cmd.expected_metadata.upstream_table_names,
        ),
        commands.CheckTableDefinition(
            table_name=cmd.expected_metadata.table_name,
        ),
        commands.CheckTablePartitionField(
            table_name=cmd.expected_metadata.table_name,
        ),
        commands.CheckTableSchema(
            table_name=cmd.expected_metadata.table_name,
            expected_schema=cmd.expected_metadata.schema,
        ),
        commands.CheckTablePartitionsAreNotEmpty(table_name=cmd.expected_metadata.table_name),
        commands.CheckTablePartitions(
            table_name=cmd.expected_metadata.table_name,
            expected_partitions=cmd.expected_metadata.partitions,
        ),
        events.TableUpToDate(table_name=cmd.expected_metadata.table_name),
    ]


def create_table(
    cmd: commands.CreateTable,
    table_repository: AbstractTableRepository,
    table_config_repository: AbstractTableConfigRepository,
) -> events.Event | errors.Error:
    try:
        table_metadata: value_objects.TableMetadata = table_repository.get_table_metadata(
            table_name=cmd.table_name
        )
        return errors.TableAlreadyExists(table_metadata=table_metadata)
    except exceptions.TableDoesNotExistError:
        table_config: value_objects.TableConfig = table_config_repository.get_table_config(
            table_name=cmd.table_name
        )
        table_repository.create_table(table_config=table_config)
        return events.TableCreated(
            table_name=cmd.table_name,
            schema=table_config.schema,
            partition_field=table_config.partition_field,
        )


def check_for_new_upstream_dependencies(
    cmd: commands.CheckForNewUpstreamDependencies,
    table_repository: AbstractTableRepository,
):
    upstream_tables: list[value_objects.TableMetadata] = [
        table_repository.get_table_metadata(table_name) for table_name in cmd.upstream_table_names
    ]
    downstream_table: value_objects.TableMetadata = table_repository.get_table_metadata(
        table_name=cmd.table_name
    )
    if any(
        [upstream_table.created > downstream_table.updated for upstream_table in upstream_tables]
    ):
        return errors.NewUpstreamDependenciesSinceLastUpdate(table_name=cmd.table_name)
    return events.NoNewUpstreamDependencies(table_name=cmd.table_name)


def copy_table(cmd: commands.CopyTable, table_repository: AbstractTableRepository):
    table_repository.copy_table(
        source_table_name=cmd.source_table_name,
        destination_table_name=cmd.destination_table_name,
        expires=cmd.expires,
    )
    return events.TableCopied(
        source_table_name=cmd.source_table_name,
        destination_table_name=cmd.destination_table_name,
    )


def plan_sideload(
    cmd: commands.PlanSideload,
    query_repository: AbstractQueryRepository,
    table_config_repository: AbstractTableConfigRepository,
) -> Sequence[commands.Command]:
    table_config: value_objects.TableConfig = table_config_repository.get_table_config(
        table_name=cmd.table_name
    )
    sideload_table_name: str = f'{cmd.table_name}_sideload_{query_repository.get_query_hash(query_name=table_config.table_name)}'
    backup_table_name: str = f'{cmd.table_name}_backup'

    query_repository.copy_query(
        source_query_name=cmd.table_name, destination_query_name=sideload_table_name
    )
    expected_metadata = value_objects.TableConfig(
        table_name=sideload_table_name,
        schema=table_config.schema,
        partition_field=table_config.partition_field,
        partitions=table_config.partitions,
        definition=table_config.definition,
    )

    return [
        commands.CheckTableState(expected_metadata=expected_metadata),
        commands.DeleteTable(table_name=backup_table_name, not_found_ok=True),
        commands.CopyTable(
            source_table_name=table_config.table_name,
            destination_table_name=backup_table_name,
        ),
        commands.ReplaceTable(
            table_name=table_config.table_name, replacement_table_name=sideload_table_name
        ),
        commands.DeleteTable(table_name=sideload_table_name),
    ]


def replace_table(cmd: commands.ReplaceTable) -> Sequence[commands.Command | events.Event]:
    return [
        commands.DeleteTable(table_name=cmd.table_name),
        commands.CopyTable(
            source_table_name=cmd.replacement_table_name, destination_table_name=cmd.table_name
        ),
        events.TableReplaced(table_name=cmd.table_name),
    ]


def plan_backfill(
    cmd: commands.PlanBackfill,
    query_repository: AbstractQueryRepository,
) -> Sequence[commands.Command]:
    cmds: MutableSequence[commands.Command] = []
    for partition in cmd.partitions:
        cmds.append(
            commands.UpdateTablePartition(
                table_name=cmd.table_name,
                query=query_repository.get_query(
                    query_name=cmd.table_name,
                    run_day=partition,
                    run_time_template_fields={'table_name': cmd.table_name},
                ),
                partition=partition,
            )
        )
    return cmds


def update_table_partition(
    cmd: commands.UpdateTablePartition, table_repository: AbstractTableRepository
):
    table_repository.write_query_results_to_table_partition(
        table_name=cmd.table_name, query=cmd.query, partition=cmd.partition
    )
    return events.TablePartitionUpdated(
        table_name=cmd.table_name, query=cmd.query, partition=cmd.partition
    )


def delete_table(cmd: commands.DeleteTable, table_repository: AbstractTableRepository):
    table_repository.delete_table(table_name=cmd.table_name, not_found_ok=cmd.not_found_ok)
    return events.TableDeleted(table_name=cmd.table_name)


def add_query(cmd: commands.AddQuery, query_repository: AbstractQueryRepository):
    query_repository.add_query(query_name=cmd.query_name, query_renderer=cmd.query_renderer)
    return events.QueryAdded(query_name=cmd.query_name)


def sync_partitioned_table(
    cmd: commands.SyncPartitionedTable,
) -> Sequence[commands.Command | events.Event]:
    return [
        commands.AddQuery(
            query_name=cmd.expected_metadata.table_name, query_renderer=cmd.query_renderer
        ),
        commands.CheckTableState(expected_metadata=cmd.expected_metadata),
        events.TableSynchronized(table_name=cmd.expected_metadata.table_name),
    ]


def sync_unpartitioned_table(
    cmd: commands.SyncUnpartitionedTable, table_repository: AbstractTableRepository
) -> events.TableSynchronized:
    table_repository.write_query_results_to_table(table_name=cmd.table_name, query=cmd.query)
    return events.TableSynchronized(table_name=cmd.table_name)


def complete_event_loop(event: events.Event):
    pass


CommandHandlers = dict[
    type[commands.Command],
    Callable[
        ...,
        Sequence[commands.Command | events.Event] | events.Event | errors.Error,
    ],
]
EventHandlers = dict[type[events.Event], Callable[..., Any]]
ErrorHandlers = dict[type[errors.Error], Callable[..., commands.Command]]

default_command_handlers: CommandHandlers = {
    commands.CheckTableState: check_table_state,
    commands.UpdateTablePartition: update_table_partition,
    commands.PlanBackfill: plan_backfill,
    commands.CreateTable: create_table,
    commands.CopyTable: copy_table,
    commands.DeleteTable: delete_table,
    commands.PlanSideload: plan_sideload,
    commands.CheckTableExists: check_table_exists,
    commands.CheckTableDefinition: check_table_definition,
    commands.CheckTablePartitionField: check_table_partition_field,
    commands.CheckTablePartitions: check_table_partitions,
    commands.CheckTablePartitionsAreNotEmpty: check_table_partitions_are_not_empty,
    commands.CheckTableSchema: check_table_schema,
    commands.CheckForNewUpstreamDependencies: check_for_new_upstream_dependencies,
    commands.AddQuery: add_query,
    commands.ReplaceTable: replace_table,
    commands.SyncPartitionedTable: sync_partitioned_table,
    commands.SyncUnpartitionedTable: sync_unpartitioned_table,
}

default_error_handlers: ErrorHandlers = {
    errors.PartitionsDoNotMatchExpectation: trigger_backfill_plan,
    errors.TableHasNoPartitions: trigger_backfill_plan,
    errors.PartitionFieldDoesNotMatchExpectation: trigger_sideload_plan,
    errors.SchemaDoesNotMatchExpectation: trigger_sideload_plan,
    errors.DefinitionDoesNotMatchExpectation: trigger_sideload_plan,
    errors.TableDoesNotExist: trigger_table_creation,
    errors.NewUpstreamDependenciesSinceLastUpdate: trigger_sideload_plan,
    # TODO: ExistingPartitionsExceedExpectations handling needs to be formalized.
    # Do we always want to sideload if existing table has excess partition?
    # (usually occurs when start date is moved up.)
    errors.ExistingPartitionsExceedExpectations: trigger_sideload_plan,
}
