import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from src.pipelines._internal import search


class TestFindAction:
    def test_can_find_existing_action(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub_module = tmp_path / 'stub_module.py'
        stub_init = tmp_path / '__init__.py'
        stub_module.touch()
        stub_init.touch()

        with stub_module.open('w+') as f:
            f.write('def stub_function(): return True')
        sys.path.insert(0, str(tmp_path.resolve()))

        stub_function: Callable[..., Any] = search.find_function(
            name='stub_function', directory=tmp_path
        )

        assert stub_function()

    def test_raises_if_action_does_not_exist(self, tmp_path: Path) -> None:
        with pytest.raises(
            expected_exception=NotImplementedError,
            match=r'.*non_existant_function not found.*',
        ):
            search.find_function(name='non_existant_function', directory=tmp_path)
