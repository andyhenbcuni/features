from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from src.pipelines import pipeline, task, trigger
from src.pipelines._internal import search


@dataclass
class Yaml:
    def compile(
        self,
        object: object,
    ) -> str:
        match object:
            case pipeline.Pipeline():
                trigger = object.trigger.__dict__ if object.trigger is not None else None
                artifact = {
                    'port': 'pipeline',
                    'name': object.name,
                    'trigger': trigger,
                    'tasks': [self._task_to_dict(task=task) for task in object.tasks],
                }
            case task.Task():
                artifact = self._task_to_dict(task=object)
            case _:
                raise NotImplementedError

        return yaml.dump(data=artifact, sort_keys=False)

    def decompile(self, artifact: str, object: type):
        config: dict[str, Any] = yaml.safe_load(stream=artifact)
        if 'trigger' in config.keys():
            if config['trigger'] is not None:
                _trigger = trigger.CronTrigger(**config['trigger'])
            else:
                _trigger = None
        else:
            _trigger = None
        match object:
            case pipeline.Pipeline:
                return object(
                    name=config['name'],
                    trigger=_trigger,
                    tasks=[self._dict_to_task(task_dict=task) for task in config['tasks']],
                )
            case task.Task:
                return self._dict_to_task(task_dict=config)
            case _:
                raise NotImplementedError

    def _task_to_dict(self, task: task.Task) -> dict[str, Any]:
        return {
            'name': task.name,
            'src': str(task.src),
            'action': task.action.__name__,
            'parameters': task.parameters,
            'depends_on': task.depends_on,
            'retries': task.retries,
            'retry_delay': task.retry_delay.total_seconds(),
        }

    def _dict_to_task(self, task_dict: dict[str, Any]) -> task.Task:
        action = search.find_function(name=task_dict['action'], directory=Path(task_dict['src']))

        return task.Task(
            name=task_dict['name'],
            action=action,
            parameters=task_dict.get('parameters', {}),
            depends_on=task_dict.get('depends_on', []),
            src=task_dict['src'],
            retries=task_dict.get('retries', 3),
            retry_delay=timedelta(seconds=task_dict.get('retry_delay', 60)),
        )
