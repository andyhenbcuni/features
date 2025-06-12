import inspect
from collections.abc import Callable
from logging import Logger
from typing import Any

from src.common import paths
from src.pipelines import pipeline
from src.scripts import utils

LOGGER: Logger = utils.get_logger(name='check_configs')


# TODO: should be async
# TODO: this is really slapdash, should be formalized when time permits
def check_actions_have_all_parameters(
    config_name: str,
    task_name: str,
    action: Callable[[Any], Any],
    parameters: dict[str, Any],
    errors: list[str],
) -> list[str]:
    run_time_parameters = {'run_day': 'unused'}  # TODO: this needs to be centralize
    all_available_parameters = set((parameters | run_time_parameters).keys())
    expected_parameters = inspect.signature(action).parameters
    required_parameters: set[str] = {
        name
        for name, parameter in expected_parameters.items()
        if parameter.default == inspect.Parameter.empty
        and parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }

    if not set(required_parameters).issubset(set(all_available_parameters)):
        errors.append(
            f'Config: {config_name}, task: {task_name}, action: {action.__name__} is missing required parameters: {required_parameters.difference(all_available_parameters)}.'
        )
    return errors


def check_configs() -> None:
    errors: list[str] = []
    for name in paths.get_config_names():
        LOGGER.info(f'Checking config: {name}...')
        p: pipeline.Pipeline = pipeline.Pipeline.from_config(name=name)
        for task in p.tasks:
            errors = check_actions_have_all_parameters(
                config_name=p.name,
                task_name=task.name,
                action=task.action,
                parameters=task.parameters,
                errors=errors,
            )
    if errors:
        formatted_errors = '\n'.join(errors)
        msg = f'Some actions are missing parameters.\n{formatted_errors}'
        raise TypeError(msg)


if __name__ == '__main__':
    LOGGER.info('Checking configs...')
    check_configs()
    LOGGER.info('All configs are valid.')
