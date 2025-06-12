import sys
from pathlib import Path

import pytest

import setup_devenv


@pytest.mark.skipif(condition=sys.platform == 'win32', reason='Path does not work across os')
@pytest.mark.integration
def test_create_venv(tmp_path: Path):
    venv_path: Path = tmp_path / '.venv'
    setup_devenv.create_venv(path=venv_path)
    assert venv_path.exists()
    assert (venv_path / 'bin' / 'python').exists()


def test_install_main_deps_fails_no_venv_at_path(tmp_path: Path):
    venv_path: Path = tmp_path / '.venv'
    assert not venv_path.exists()

    with pytest.raises(ValueError):
        setup_devenv.install_main_deps(venv_path)


def test_install_dev_deps_fails_no_venv_at_path(tmp_path: Path):
    venv_path: Path = tmp_path / '.venv'
    assert not venv_path.exists()

    with pytest.raises(ValueError):
        setup_devenv.install_dev_deps(venv_path)
