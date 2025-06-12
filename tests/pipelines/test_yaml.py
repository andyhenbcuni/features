import pathlib
from contextlib import nullcontext as does_not_raise
from datetime import timedelta

import yaml as pyyaml

from src.pipelines import pipeline, port, task
from src.pipelines.compilers import yaml
from tests.pipelines import pipelines_helpers


class TestTask:
    def test_can_compile(self) -> None:
        compiler = yaml.Yaml()
        stub_task = task.Task(
            name='stub_task',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
        )
        expected_yaml: str = (
            f'name: {stub_task.name}\n'
            f'src: {stub_task.src}\n'
            'action: stub_action\n'
            'parameters:\n'
            '  stub_parameter: stub_value\n'
            'depends_on:\n'
            '- upstream_task\n'
            '- other_upstream_task\n'
            f'retries: {stub_task.retries}\n'
            f'retry_delay: {stub_task.retry_delay.total_seconds()}\n'
        )

        actual_yaml: str = compiler.compile(object=stub_task)

        assert actual_yaml == expected_yaml
        with does_not_raise():
            _ = pyyaml.safe_load(actual_yaml)

    def test_decompile(self) -> None:
        compiler = yaml.Yaml()
        artifact: str = (
            'name: stub_task\n'
            f'src: {pathlib.Path(__file__).parent}\n'
            'action: stub_action\n'
            'parameters:\n'
            '  stub_parameter: stub_value\n'
            'depends_on:\n'
            '- upstream_task\n'
            '- other_upstream_task\n'
        )
        expected_task = task.Task(
            name='stub_task',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
        )

        actual_task: port.Port = compiler.decompile(artifact=artifact, object=task.Task)

        assert actual_task == expected_task


class TestPipeline:
    def test_can_compile_single_task(self) -> None:
        compiler = yaml.Yaml()
        stub_task = task.Task(
            name='stub_task',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
        )
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task])
        expected_yaml: str = (
            'port: pipeline\n'
            'name: stub_pipeline\n'
            'trigger: null\n'
            'tasks:\n'
            f'- name: {stub_task.name}\n'
            f'  src: {stub_task.src}\n'
            '  action: stub_action\n'
            '  parameters:\n'
            '    stub_parameter: stub_value\n'
            '  depends_on:\n'
            '  - upstream_task\n'
            '  - other_upstream_task\n'
            f'  retries: {stub_task.retries}\n'
            f'  retry_delay: {stub_task.retry_delay.total_seconds()}\n'
        )

        actual_yaml: str = compiler.compile(object=stub_pipeline)

        assert actual_yaml == expected_yaml
        with does_not_raise():
            _ = pyyaml.safe_load(actual_yaml)

    def test_can_compile_multiple_tasks(self) -> None:
        compiler = yaml.Yaml()
        stub_task = task.Task(
            name='stub_task',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
            retries=2,
            retry_delay=timedelta(minutes=2),
        )
        stub_task_2 = task.Task(
            name='stub_task_2',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
            retries=4,
            retry_delay=timedelta(minutes=2),
        )
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task, stub_task_2])
        expected_yaml: str = (
            'port: pipeline\n'
            'name: stub_pipeline\n'
            'trigger: null\n'
            'tasks:\n'
            f'- name: {stub_task.name}\n'
            f'  src: {stub_task.src}\n'
            '  action: stub_action\n'
            '  parameters:\n'
            '    stub_parameter: stub_value\n'
            '  depends_on:\n'
            '  - upstream_task\n'
            '  - other_upstream_task\n'
            f'  retries: {stub_task.retries}\n'
            f'  retry_delay: {stub_task.retry_delay.total_seconds()}\n'
            f'- name: {stub_task_2.name}\n'
            f'  src: {stub_task_2.src}\n'
            '  action: stub_action\n'
            '  parameters:\n'
            '    stub_parameter: stub_value\n'
            '  depends_on:\n'
            '  - upstream_task\n'
            '  - other_upstream_task\n'
            f'  retries: {stub_task_2.retries}\n'
            f'  retry_delay: {stub_task_2.retry_delay.total_seconds()}\n'
        )

        actual_yaml: str = compiler.compile(object=stub_pipeline)

        assert actual_yaml == expected_yaml

    def test_can_decompile(self) -> None:
        compiler = yaml.Yaml()
        stub_task = task.Task(
            name='stub_task',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
        )
        stub_task_2 = task.Task(
            name='stub_task_2',
            action=pipelines_helpers.stub_action,
            parameters={'stub_parameter': 'stub_value'},
            depends_on=['upstream_task', 'other_upstream_task'],
            src=pathlib.Path(__file__).parent,
        )
        expected_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task, stub_task_2])
        artifact: str = (
            'port: pipeline\n'
            'name: stub_pipeline\n'
            'trigger: null\n'
            'tasks:\n'
            f'- name: {stub_task.name}\n'
            f'  src: {stub_task.src}\n'
            '  action: stub_action\n'
            '  parameters:\n'
            '    stub_parameter: stub_value\n'
            '  depends_on:\n'
            '  - upstream_task\n'
            '  - other_upstream_task\n'
            f'- name: {stub_task_2.name}\n'
            f'  src: {stub_task_2.src}\n'
            '  action: stub_action\n'
            '  parameters:\n'
            '    stub_parameter: stub_value\n'
            '  depends_on:\n'
            '  - upstream_task\n'
            '  - other_upstream_task\n'
        )

        actual_pipeline: port.Port = compiler.decompile(artifact=artifact, object=pipeline.Pipeline)

        assert actual_pipeline == expected_pipeline
