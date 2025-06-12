import inspect
import pathlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from src.common import paths
from src.pipelines import adapters, port, task, utils
from src.pipelines import trigger as trigger_base
from src.pipelines._internal import graph

logger = utils.get_logger(name='port')


@dataclass
class Pipeline(port.Port):
    name: str
    trigger: trigger_base.Trigger | None = None
    tasks: list[task.Task] = field(default_factory=list)
    log: list[task.Task] = field(default_factory=list, init=False, compare=False)

    def __getitem__(self, key: str) -> task.Task:
        return self.task_dict[key]

    @property  # TODO: cache?
    def task_dict(self) -> dict[str, task.Task]:
        return {task.name: task for task in self.tasks}

    def run(
        self,
        run_time_parameters: dict[str, Any] | None = None,
        task_names: list[str] | None = None,
    ) -> None:
        nodes: list[task.Task] = (
            [self.task_dict[name] for name in task_names] if task_names else self.tasks
        )
        dag = graph.DAG(nodes=nodes)
        for node in dag:
            self.log.append(node)
            node.action(**node.parameters | (run_time_parameters or {}))

    def show(self) -> utils.RenderMermaid:
        """Renders a graphical representation of the pipeline."""
        if not self.tasks:
            print(
                'Nothing to show! No tasks have been added to the pipeline.\n\tTry adding tasks to the pipeline using pipeline.add_task()'
            )
        return utils.RenderMermaid(diagram=self.compile(adapter=adapters.Adapters.MERMAID))

    def add_task(  # noqa: PLR0913
        self,
        name: str,
        action: Callable[..., Any],
        parameters: dict[str, Any] | None = None,
        before: str | list[str] | None = None,
        after: str | list[str] | None = None,
    ) -> None:
        dependencies: list[str] = self._convert_to_list(candidate=after)
        self._validate_dependencies(task_name=name, dependencies=dependencies)
        self._add_upstream_dependency(
            new_dependency_name=name,
            tasks_to_modify=self._convert_to_list(candidate=before),
        )

        self.tasks.append(
            task.Task(
                name=name,
                action=action,
                parameters=parameters or {},
                depends_on=dependencies,
                src=pathlib.Path(
                    inspect.getfile(action),
                )
                .relative_to(pathlib.Path(__file__).parent.parent.parent)
                .parent,
            )
        )

    def remove_task(self, name: str) -> None:
        self.tasks = [task for task in self.tasks if task.name != name]

    def _add_upstream_dependency(
        self, new_dependency_name: str, tasks_to_modify: list[str]
    ) -> None:
        self._validate_dependencies(task_name=new_dependency_name, dependencies=tasks_to_modify)
        for task_name in tasks_to_modify:
            self.task_dict[task_name].depends_on.append(new_dependency_name)

    def _validate_dependencies(self, task_name: str, dependencies: list[str]) -> None:
        for node in dependencies:  # TODO: use sets, and issubset
            if node not in self.task_dict:
                raise ValueError(
                    f'Cannot run: {task_name} after: {node} because node does not exist in the pipeline.\nExisting nodes: {self.task_dict.keys()}'
                )

    @staticmethod
    def _convert_to_list(candidate: str | list[str] | None) -> list[str]:
        if isinstance(candidate, list):
            return candidate
        return [candidate] if candidate else []

    # TODO: this should be to/from file and each adapter should maintain its own directories (yaml etc)
    def to_config(
        self, adapter: adapters.Adapters | None = None, config_directory: pathlib.Path | None = None
    ):
        config_directory = config_directory or paths.get_path('configs')
        adapter = adapter or adapters.Adapters.YAML
        artifact_candidates = list(
            config_directory.glob(f'*/{self.name}.*')
        )  # TODO: need helper for finding config by name
        if len(artifact_candidates) > 1:
            msg = f'More than one config found matching the name: {self.name}. Found: {artifact_candidates}.'
            raise ValueError(msg)

        if len(artifact_candidates) == 1:
            artifact_path = artifact_candidates[0]
            msg = f'Config found matching the name: {self.name} at path: {artifact_path}.'
            logger.warning(msg)
        else:
            artifact_dir = config_directory / self.name
            artifact_dir.mkdir(parents=True)
            artifact_path = artifact_dir / f'{self.name}.yaml'
            artifact_path.touch()

        with artifact_path.open('w') as f:
            f.write(self.compile(adapter=adapter))
