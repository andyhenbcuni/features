import pytest

from src.pipelines import pipeline, task
from tests.pipelines import pipelines_helpers


class TestPipeline:
    def test_task_dict(self) -> None:
        stub_task = task.Task(name='stub_task', action=pipelines_helpers.stub_action)
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task])

        assert stub_pipeline.task_dict == {'stub_task': stub_task}

    def test_runs_expected_number_of_tasks(self) -> None:
        stub_task_1 = task.Task(name='stub_task_1', action=pipelines_helpers.stub_action)
        stub_task_2 = task.Task(name='stub_task_2', action=pipelines_helpers.stub_action)
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task_1, stub_task_2])

        stub_pipeline.run()

        assert len(stub_pipeline.log) == 2

    def test_runs_tasks_in_expected_order(self) -> None:
        stub_task_1 = task.Task(name='stub_task_1', action=pipelines_helpers.stub_action)
        stub_task_2 = task.Task(
            name='stub_task_2', action=pipelines_helpers.stub_action, depends_on=['stub_task_1']
        )
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[stub_task_1, stub_task_2])

        stub_pipeline.run()

        assert stub_pipeline.log == [stub_task_1, stub_task_2]

    def test_add_task(self) -> None:
        stub_task_name = 'stub_task'
        expected_tasks: list[task.Task] = [
            task.Task(name=stub_task_name, action=pipelines_helpers.stub_action)
        ]
        stub_pipeline = pipeline.Pipeline(name='stub_pipeline')

        stub_pipeline.add_task(name=stub_task_name, action=pipelines_helpers.stub_action)

        assert stub_pipeline.tasks == expected_tasks

    def test_remove_task(self):
        stub_task_name = 'stub_task'
        stub_pipeline = pipeline.Pipeline(
            name='stub_pipeline',
            tasks=[task.Task(name=stub_task_name, action=pipelines_helpers.stub_action)],
        )

        stub_pipeline.remove_task(name=stub_task_name)

        assert stub_pipeline.tasks == []

    class TestValidateDependencies:
        def test_raises_if_dependency_does_not_exist(self):
            stub_pipeline = pipeline.Pipeline(name='stub_pipeline', tasks=[])

            with pytest.raises(ValueError):
                stub_pipeline._validate_dependencies(
                    task_name='stub_task', dependencies=['stub_dependency']
                )

        def test_does_not_raise_if_dependency_does_exist(self):
            stub_task_name = 'stub_task'

            stub_pipeline = pipeline.Pipeline(
                name='stub_pipeline',
                tasks=[task.Task(name=stub_task_name, action=pipelines_helpers.stub_action)],
            )

            stub_pipeline.remove_task(name=stub_task_name)

    def test_add_upstream_dependency(self):
        stub_task_name = 'stub_task'
        stub_upstream_task_name = 'stub_upstream_task_name'
        stub_pipeline = pipeline.Pipeline(
            name='stub_pipeline',
            tasks=[task.Task(name=stub_task_name, action=pipelines_helpers.stub_action)],
        )

        stub_pipeline._add_upstream_dependency(
            new_dependency_name=stub_upstream_task_name,
            tasks_to_modify=[stub_task_name],
        )

        assert stub_pipeline[stub_task_name].depends_on == [stub_upstream_task_name]

    class TestConvertToList:
        def test_from_none(self) -> None:
            assert pipeline.Pipeline._convert_to_list(candidate=None) == []

        def test_from_str(self) -> None:
            assert pipeline.Pipeline._convert_to_list(candidate='stub') == ['stub']

        def test_from_list(self) -> None:
            assert pipeline.Pipeline._convert_to_list(candidate=['stub']) == ['stub']
