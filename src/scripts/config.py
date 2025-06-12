import dataclasses
from pathlib import Path

from dynaconf import Dynaconf


@dataclasses.dataclass
class AlgoFeaturesConfig:
    dags_dir: Path
    is_manual_test_env: bool


_dynaconf_settings = Dynaconf(
    root_path='src/scripts',
    envvar_prefix='DYNACONF',
    settings_files=['settings.toml', '.secrets.toml'],
)
# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar` (take precidence over settings files)
# `settings_files` = Load these files in the order.


settings = AlgoFeaturesConfig(
    dags_dir=Path(_dynaconf_settings.dags_dir),
    is_manual_test_env=bool(_dynaconf_settings.is_manual_test_env),
)
