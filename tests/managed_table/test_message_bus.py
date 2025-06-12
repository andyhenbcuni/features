from collections.abc import Iterator
from contextlib import nullcontext as does_not_raise

import pytest

from src.managed_table.domain import commands, errors, events
from src.managed_table.services import message_bus


def pass_event(event: events.Event): ...


class TestCommand(commands.Command): ...


class ErrorCommand(commands.Command): ...


class FirstEvent(events.Event): ...


class PassEvent(events.Event): ...


class TestError(errors.Error): ...


def test_message_bus_inserts_responses_into_queue() -> None:
    command = TestCommand()
    first_event = FirstEvent()
    middle_event = PassEvent()
    later_event = PassEvent()

    def command_handler(cmd: TestCommand) -> list[events.Event]:
        return [first_event, later_event]

    def first_event_handler(event: FirstEvent) -> events.Event:
        return middle_event

    bus = message_bus.MessageBus(
        event_handlers={
            FirstEvent: first_event_handler,
            PassEvent: pass_event,
        },
        command_handlers={TestCommand: command_handler},
        error_handlers={},
    )

    bus.dispatch(message=command)

    assert bus.log == [command, first_event, middle_event, later_event]


@pytest.mark.regression
def test_message_bus_handles_errors_first_then_dead_letter_queue_then_queue() -> None:
    command = TestCommand()
    error_command = ErrorCommand()
    error_command_event = PassEvent()
    first_event = FirstEvent()
    error = TestError()
    later_event = PassEvent()
    error_generator: Iterator[TestError | None] = iter([error, None])

    def command_handler(cmd: commands.Command) -> list[events.Event]:
        return [first_event, later_event]

    def first_event_handler(event: events.Event) -> TestError | None:
        return next(error_generator)

    def error_handler(error: errors.Error) -> commands.Command:
        return error_command

    def error_command_handler(cmd: commands.Command) -> events.Event:
        return error_command_event

    bus = message_bus.MessageBus(
        event_handlers={
            FirstEvent: first_event_handler,
            PassEvent: pass_event,
        },
        command_handlers={TestCommand: command_handler, ErrorCommand: error_command_handler},
        error_handlers={
            TestError: error_handler,
        },
    )

    with does_not_raise():
        bus.dispatch(message=command)

        assert bus.log == [
            command,
            first_event,
            error,
            error_command,
            error_command_event,
            first_event,  # retried from dead_letter_queue
            later_event,
        ]


@pytest.mark.regression
def test_message_bus_dead_letter_queue_recurring_error_raises_sys_exit() -> None:
    command = TestCommand()
    error_command = ErrorCommand()
    first_event = FirstEvent()
    error = TestError()
    later_event = PassEvent()

    def command_handler(cmd: commands.Command) -> list[events.Event]:
        return [first_event, later_event]

    def first_event_handler(event: events.Event) -> TestError:
        return error

    def error_handler(error: errors.Error) -> commands.Command:
        return error_command

    def error_command_handler(cmd: commands.Command) -> TestError:
        # Recurring errors
        return error

    bus = message_bus.MessageBus(
        event_handlers={
            FirstEvent: first_event_handler,
            PassEvent: pass_event,
        },
        command_handlers={TestCommand: command_handler, ErrorCommand: error_command_handler},
        error_handlers={
            TestError: error_handler,
        },
    )

    with pytest.raises(SystemExit, match='Maximum number of retries reached, terminating program'):
        bus.dispatch(message=command)
