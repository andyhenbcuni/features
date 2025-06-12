import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common import paths
from src.pipelines import adapters

'a task is an action with a name and dependencies'

root_module = 'src.pipelines.compilers.{}'


@dataclass
class Port:
    def compile(
        self,
        adapter: adapters.Adapters,
    ) -> str:
        # TODO: these will eventually need to be injected probably
        compiler_module = importlib.import_module(root_module.format(adapter.value))
        compiler = getattr(compiler_module, adapter.value.capitalize())
        return compiler().compile(object=self)

    @classmethod
    def decompile(cls, artifact: Any, adapter: adapters.Adapters):
        # TODO: these will eventually need to be injected probably
        compiler_module = importlib.import_module(root_module.format(adapter.value))
        compiler = getattr(compiler_module, adapter.value.capitalize())
        return compiler().decompile(artifact=artifact, object=cls)

    @classmethod
    def from_config(
        cls,
        name: str,
        adapter: adapters.Adapters | None = None,
        config_directory: Path | None = None,
    ):
        config_directory = config_directory or paths.get_path('configs')
        adapter = adapter or adapters.Adapters.YAML
        artifact_candidates = list(
            config_directory.glob(f'*/{name}.*')
        )  # TODO: need helper for finding config by name
        if len(artifact_candidates) > 1:
            msg = f'More than one config found matching the name: {name}. Found: {artifact_candidates}.'
            raise ValueError(msg)
        if len(artifact_candidates) == 0:
            msg = f'No configs found matching the name: {name}.'
            raise ValueError(msg)

        artifact_path = artifact_candidates[0]

        with artifact_path.open() as f:
            artifact = f.read()
        return cls.decompile(artifact=artifact, adapter=adapter)
