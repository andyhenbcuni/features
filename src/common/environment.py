"Module for environment dependent variables."

import os
from pathlib import Path
from typing import Final, Literal

BQ_DATASET: Final[Literal['algo_features']] = 'algo_features'
ROOT: Path = Path.cwd()

os.environ.setdefault(key='AIRFLOW__CORE__DAGS_FOLDER', value=str(object=ROOT))
ALGO_PROJECT: Final[str] = os.environ.get(
    'AIRFLOW_VAR_ALGO_PROJECT', default='nbcu-ds-algo-int-001'
)
BQ_BILLING_PROJECT: Final[str] = ALGO_PROJECT
BQ_DATA_ENV: Final[str] = 'nbcu-ds-prod-001'

DB_CATALOG: str = 'DB_CATALOG'
DB_SCHEMA: str = 'DB_SCHEMA'

PROJECT_DIR: Path = Path(os.environ['AIRFLOW__CORE__DAGS_FOLDER']) / 'src/'
CONFIG_DIR: Path = PROJECT_DIR / 'configs'


BQ_DESTINATION: str = f'{BQ_BILLING_PROJECT}.{BQ_DATASET}.{{table_id}}'
DB_DESTINATION: str = f'{DB_CATALOG}.{DB_SCHEMA}.{{table_name}}'
