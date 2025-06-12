import shlex
import subprocess
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Final
from unittest import mock

from src.managed_table.domain import commands
from src.managed_table.services import handlers

ROOT_DIR: Final[Path] = Path(__file__).parent.parent.parent.parent.parent
ENTRYPOINT_DIR: Final[Path] = ROOT_DIR / 'pipelines' / 'resources' / 'bigquery'


@mock.patch(
    'src.managed_table.entrypoints.local.bootstrap.handlers.default_command_handlers',
    {commands.CheckTableState: handlers.complete_event_loop},
)
def test_sync_table() -> None:
    with does_not_raise():
        cmd: list[str] = shlex.split(
            f"""python {ENTRYPOINT_DIR} entrypoint table_name='table_name' schema='[["test", "schema"], ["another", "one"]]' partition_field='partition_field' start_date='2024-01-01' end_date='2024-01-01' query='unused'"""
        )
        subprocess.run(
            args=cmd,
            check=False,
        )
