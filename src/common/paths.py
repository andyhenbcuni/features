import os
from collections.abc import Generator
from pathlib import Path


def get_path(path_type: str) -> Path:
    src = Path(os.environ.get('base_path') or Path(__file__).parent.parent)
    paths: dict[str, Path] = {
        'configs': src / 'configs',
        'dag_jinja': Path(__file__).parent.parent
        / 'actions'
        / 'orchestrators'
        / 'pipeline'
        / 'adapters'
        / 'airflow'
        / 'dag.py.jinja2',
    }

    if path_type in paths:
        return paths[path_type]
    else:
        raise ValueError(
            f"Invalid path type: {path_type}. Available types: {', '.join(paths.keys())}"
        )


def get_config_names() -> Generator[str, None, None]:
    config_dir: Path = get_path(path_type='configs')
    return (
        path.with_suffix(suffix='').name
        for path in config_dir.rglob(pattern='*.yaml')
        if 'sql' not in path.parts
    )
