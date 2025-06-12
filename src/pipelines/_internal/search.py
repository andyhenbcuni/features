"Utility functions for searching for and parsing python objects in directories."

import ast
import importlib
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn

ROOT: Path = Path(__file__).parent.parent

SEARCH_EXCLUSIONS: set[Path] = {Path('.venv'), Path('venv'), Path('.*'), Path('*.ipynb')}


def find_function(name: str, directory: Path) -> Callable[..., Any] | NoReturn:
    """
    Recursively searchs for a function in a given directory.

    Args:
        name (str): name of the function to find.
        directory (Path, optional): directory to search. Defaults to DEFAULT_SEARCH_DIRECTORY.

    Raises:
        NotImplementedError: raises if function is not found in the given aeidirectory.

    Returns:
        Callable[..., Any]: the requested function, if found.
    """
    base = Path(__file__).parent.parent.parent.parent
    directory = base / directory

    for path in directory.rglob(pattern='*.py'):
        if path not in SEARCH_EXCLUSIONS:
            module_path: str = module_path_from_pathlib_path(
                full_module_path=directory, base_path=path
            )
            if module_path in sys.modules:
                module: types.ModuleType = sys.modules[module_path]
                if hasattr(module, name):
                    return getattr(module, name)
            sys.path.insert(0, str(directory))
            parsed_module: ast.Module = _parse_module(path=path)

            for node in ast.walk(node=parsed_module):
                if isinstance(node, ast.FunctionDef) and node.name == name:
                    module: types.ModuleType = importlib.import_module(name=module_path)
                    return getattr(module, name)

    raise NotImplementedError(
        f'Action: {name} not found in any subdirectory of action_path: {directory}'
    )


def module_path_from_pathlib_path(full_module_path: Path, base_path: Path) -> str:
    return '.'.join(base_path.relative_to(full_module_path).with_suffix(suffix='').parts)


def _parse_module(path: Path) -> ast.Module:
    with path.open() as module:
        return ast.parse(source=module.read())
