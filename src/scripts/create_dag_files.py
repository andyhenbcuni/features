import shutil
from pathlib import Path
from typing import Final

from src.common import paths
from src.pipelines import adapters, pipeline
from src.scripts import config, utils

LOGGER: Final = utils.get_logger(name='create_dag_files')


def main() -> None:
    target_dir = config.settings.dags_dir.resolve()
    if not target_dir.exists():
        msg: str = f'target_dir does not exist, cannot create dag files: `{target_dir}`'
        raise ValueError(msg)

    for config_name in paths.get_config_names():
        dag: str = pipeline.Pipeline.from_config(name=config_name).compile(
            adapter=adapters.Adapters.AIRFLOW
        )
        dag_path = target_dir / f'{config_name}_dag.py'
        dag_path.touch()
        with dag_path.open('w') as f:
            f.write(dag)
        if dag_path.exists():
            LOGGER.info(f'DAG file created at: `{dag_path}`')

    # TODO: create solution for `frozen` dags that are not regenerated.
    source: Path = Path(__file__).parent / 'user_features_v1_dag.py'
    destination: Path = target_dir / 'user_features_dag.py'
    shutil.copy(src=source, dst=destination)
    LOGGER.info(f'DAG file created at: {destination}')


if __name__ == '__main__':
    main()
