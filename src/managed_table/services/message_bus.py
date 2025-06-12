import functools
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from src.managed_table import utils
from src.managed_table.domain.commands import Command
from src.managed_table.domain.errors import Error
from src.managed_table.domain.events import Event
from src.managed_table.domain.messages import Message
from src.managed_table.services import handlers
from src.managed_table.services.handlers import (
    CommandHandlers,
    ErrorHandlers,
    EventHandlers,
)

logger = utils.get_logger('message_bus')


@dataclass
class MessageBus:
    event_handlers: EventHandlers
    command_handlers: CommandHandlers
    error_handlers: ErrorHandlers
    log: list[Message] = field(default_factory=list)
    retry_count: int = 0

    def dispatch(self, message: Message) -> None:
        message_queue: list[Message] = [message]
        dead_letter_queue: list[Message] = []
        while queue := (dead_letter_queue or message_queue):
            message = queue.pop()

            self.log.append(message)
            logger.info(message)
            if response := self.handle(message=message):
                if isinstance(response, Error):
                    if self.retry_count <= 3:
                        self.retry_count += 1
                        extend_queue(message, dead_letter_queue)
                        self.dispatch(message=response)
                    else:
                        # log the error/ maximum retries reached
                        # terminate processing
                        raise SystemExit('Maximum number of retries reached, terminating program')
                else:
                    if not isinstance(response, Command):
                        self.retry_count = 0
                    extend_queue(response, queue)

    def handle(self, message: Message) -> Message | Sequence[Message]:
        handler: Callable[..., Message | Sequence[Message]] = self.get_handler(message=message)
        return handler(message)

    def get_handler(self, message: Message) -> Callable[..., Message | Sequence[Message]]:
        match message:
            case Event():
                return self.event_handlers.get(type(message), handlers.complete_event_loop)

            case Command():
                return self.command_handlers[type(message)]
            case Error():
                return self.error_handlers[type(message)]
            case _:
                raise NotImplementedError(f'No handler exists for object of type: {type(message)}')


@functools.singledispatch
def extend_queue(candidate: Any, queue: list[Message]) -> None:
    raise NotImplementedError(f'Unable to extend queue for type: {type(candidate)}')


@extend_queue.register
def _(candidate: list, queue: list[Message]) -> None:
    queue.extend(reversed(candidate))


@extend_queue.register
def _(candidate: Event, queue: list[Message]) -> None:
    queue.append(candidate)


@extend_queue.register
def _(candidate: Command, queue: list[Message]) -> None:
    queue.append(candidate)


@extend_queue.register
def _(candidate: Error, queue: list[Message]) -> None:
    queue.append(candidate)
