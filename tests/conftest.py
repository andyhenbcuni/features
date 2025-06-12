import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import yaml

os.environ['PYTHONPATH'] = './tests'
os.environ['AIRFLOW_VAR_ALGO_PROJECT'] = 'nbcu-ds-algo-int-001'
os.environ['AIRFLOW_VAR_DS_BQ_DATA_ENV'] = 'nbcu-ds-prod-001'
os.environ['AIRFLOW_VAR_CLUSTER_PROJECT'] = 'unused'
os.environ['AIRFLOW_VAR_TAG'] = 'unused'
os.environ['AIRFLOW_VAR_CLUSTER_NAME'] = 'unused'
os.environ['AIRFLOW_VAR_CLUSTER_LOCATION'] = 'unused'
os.environ['AIRFLOW_VAR_NAMESPACE'] = 'unused'
os.environ['AIRFLOW_VAR_START_DATE'] = 'unused'


def pytest_sessionfinish(session, exitstatus) -> None:
    """Enables test suite to exit with no error status if no tests are run.
    This is most common when no tests match a specified marker combo in CI.
    """
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture(scope='function')
def create_file(tmp_path: Path) -> Generator[Callable[..., Path], Any, None]:
    created_files: list[Path] = []

    def _create_file(
        filename: str, body: str, parent_dir: str | None = None, base_path: Path = tmp_path
    ) -> Path:
        if '/' in filename:
            msg: str = f'filename `{filename}` appears to have a parent dir or `/`.'
            raise ValueError(msg)
        if parent_dir:
            (base_path / parent_dir).mkdir(parents=True)
            filepath: Path = base_path / parent_dir / filename
        else:
            filepath = base_path / filename

        with filepath.open('w') as file:
            file.write(body)
        created_files.append(filepath)
        return filepath

    yield _create_file

    for path in created_files:
        if path.exists():
            path.unlink()


@pytest.fixture(scope='function')
def create_yaml_file(tmp_path: Path) -> Generator[Callable[..., Path], Any, None]:
    created_files: list[Path] = []

    def _create_file(
        filename: str, body: str, parent_dir: str | None = None, base_path: Path = tmp_path
    ) -> Path:
        if '/' in filename:
            msg: str = f'filename `{filename}` appears to have a parent dir or `/`.'
            raise ValueError(msg)
        if parent_dir:
            (base_path / parent_dir).mkdir(parents=True)
            filepath: Path = base_path / parent_dir / filename
        else:
            filepath = base_path / filename

        yaml_body = yaml.safe_load_all(stream=body)
        with filepath.open('w') as file:
            yaml.safe_dump_all(documents=yaml_body, stream=file)
        created_files.append(filepath)
        return filepath

    yield _create_file

    for path in created_files:
        if os.path.exists(path=path):
            os.remove(path=path)
