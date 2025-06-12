from collections.abc import Callable

from src.pipelines import pipeline, task
from src.pipelines.compilers import mermaid


class TestCanCompile:
    def test_pipeline_with_connected_nodes(
        self,
        get_stub_tasks: Callable[..., list[task.Task]],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
    ) -> None:
        compiler = mermaid.Mermaid()
        stub_task_1, stub_task_2 = get_stub_tasks(n=2, connected=True)
        stub_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task_1, stub_task_2])
        expected_mermaid = (
            '---\n'
            f'title: {stub_pipeline.name}\n'
            '---\n'
            'graph LR\n'
            f'{stub_task_1.name} ---> {stub_task_2.name}\n'
        )

        actual_mermaid = compiler.compile(stub_pipeline)

        assert actual_mermaid == expected_mermaid

    def test_pipeline_with_unconected_nodes(
        self,
        get_stub_tasks: Callable[..., list[task.Task]],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
    ) -> None:
        compiler = mermaid.Mermaid()
        stub_task_1, stub_task_2 = get_stub_tasks(n=2)
        stub_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task_1, stub_task_2])
        expected_mermaid: str = (
            '---\n'
            f'title: {stub_pipeline.name}\n'
            '---\n'
            'graph LR\n'
            f'{stub_task_1.name}\n'
            f'{stub_task_2.name}\n'
        )

        actual_mermaid = compiler.compile(stub_pipeline)

        assert actual_mermaid == expected_mermaid

    def test_pipeline_with_connected_and_unconnected_nodes(
        self,
        get_stub_task: Callable[..., task.Task],
        get_stub_tasks: Callable[..., list[task.Task]],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
    ) -> None:
        compiler = mermaid.Mermaid()
        stub_task_1, stub_task_2 = get_stub_tasks(n=2, connected=True)
        stub_task_3: task.Task = get_stub_task(name='stub_task_3')
        stub_pipeline: pipeline.Pipeline = get_stub_pipeline(
            tasks=[stub_task_1, stub_task_2, stub_task_3]
        )
        expected_mermaid: str = (
            '---\n'
            f'title: {stub_pipeline.name}\n'
            '---\n'
            'graph LR\n'
            f'{stub_task_1.name} ---> {stub_task_2.name}\n'
            f'{stub_task_3.name}\n'
        )

        actual_mermaid = compiler.compile(stub_pipeline)

        assert actual_mermaid == expected_mermaid


class TestCanDecompile:
    def test_pipeline_with_connected_nodes(self): ...

    def test_pipeline_with_unconected_nodes(self): ...

    def test_pipeline_with_connected_and_unconnected_nodes(self): ...
