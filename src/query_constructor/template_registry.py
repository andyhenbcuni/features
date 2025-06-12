from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

DEFAULT_TEMPLATES_DIRECTORY = Path(__file__).parent / 'templates'


def open_and_read_path(path: Path) -> str:
    with path.open() as f:
        return f.read()


def default_templates() -> dict[str, Callable[..., str]]:
    return {
        path.stem.split('.')[0]: partial(open_and_read_path, path)
        for path in DEFAULT_TEMPLATES_DIRECTORY.iterdir()
    }


@dataclass
class QueryTemplateRegistry:
    templates: dict[str, Callable[..., str]] = field(default_factory=default_templates)

    def get_template(self, name: str) -> str:
        return self.templates[name]()
