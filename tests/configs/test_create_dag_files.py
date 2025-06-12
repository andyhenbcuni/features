from pathlib import Path

import pytest

from src.scripts import config


class TestConfigSettings:
    @pytest.mark.regression
    def test_dag_dir_resolves_to_absolute_path(self):
        """
        top_level
        |
        |- cwd *
        |- dag_dir
        """

        actual: Path = config.settings.dags_dir
        expected: Path = Path('../dags_dir')

        assert actual == expected
