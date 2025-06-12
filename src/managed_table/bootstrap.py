import functools
import inspect

from src.managed_table import configs
from src.managed_table.repositories.config.adapters.local import (
    InMemoryTableConfigRepository,
)
from src.managed_table.repositories.config.base import AbstractTableConfigRepository
from src.managed_table.repositories.query.base import AbstractQueryRepository
from src.managed_table.repositories.table.base import AbstractTableRepository
from src.managed_table.services import handlers, message_bus


def bootstrap(
    table_repository: AbstractTableRepository,
    query_repository: AbstractQueryRepository,
    table_config_repository: AbstractTableConfigRepository | None = None,
) -> message_bus.MessageBus:
    if table_config_repository is None:
        table_config_repository = InMemoryTableConfigRepository()
    return message_bus.MessageBus(
        command_handlers=inject_repositories_into_default_command_handlers(
            table_repository=table_repository,
            query_repository=query_repository,
            table_config_repository=table_config_repository,
        ),
        event_handlers={},
        error_handlers=handlers.default_error_handlers,
    )


def bootstrap_from_config(config: configs.BootstrapConfig) -> message_bus.MessageBus:
    return bootstrap(
        table_repository=config.table_repository,
        query_repository=config.query_repository,
        table_config_repository=config.table_config_repository,
    )


def inject_repositories_into_default_command_handlers(
    table_repository: AbstractTableRepository,
    query_repository: AbstractQueryRepository,
    table_config_repository: AbstractTableConfigRepository,
):
    return {
        command: _inject_repositories_into_handler(
            handler=handler,
            repositories={
                'table_repository': table_repository,
                'query_repository': query_repository,
                'table_config_repository': table_config_repository,
            },
        )
        for command, handler in handlers.default_command_handlers.items()
    }


def _inject_repositories_into_handler(
    handler,
    repositories: dict[
        str, AbstractQueryRepository | AbstractTableRepository | AbstractTableConfigRepository
    ],
):
    return functools.partial(
        handler,
        **{
            key: repositories[key]
            for key in inspect.signature(handler).parameters.keys()
            if 'repository' in key
        },
    )
