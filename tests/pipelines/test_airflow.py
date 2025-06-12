import os
import pathlib
from collections.abc import Callable

import pytest
from airflow import models
from airflow.operators import empty

from src.pipelines import pipeline, port, task
from src.pipelines.compilers import airflow


def get_dag_edges(dag: models.DAG) -> set[tuple[str, str]]:
    """Get the edges for a DAG object.

    Args:
        dag (models.DAG):

    Returns:
        list[tuple[str, str]]: List of tuples representing the edges of the DAG.
    """
    return {
        (task_id, downstream_task.task_id)
        for task_id, task in dag.task_dict.items()
        for downstream_task in task.downstream_list
    }


def test_get_dag_edges(test_dag: models.DAG) -> None:
    with test_dag as dag:
        a = empty.EmptyOperator(task_id='a')
        b = empty.EmptyOperator(task_id='b')
        c = empty.EmptyOperator(task_id='c')
        d = empty.EmptyOperator(task_id='d')
        e = empty.EmptyOperator(task_id='e')

        a >> [b, c]
        c >> [d, e]

    expected_edges = {('a', 'b'), ('a', 'c'), ('c', 'd'), ('c', 'e')}

    assert get_dag_edges(dag=dag) == expected_edges


class TestPipeline:
    @pytest.mark.slow
    def test_can_compile_single_task(
        self,
        get_stub_task: Callable[..., task.Task],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
        get_dag_from_string: Callable[..., models.DAG],
    ) -> None:
        compiler = airflow.Airflow()
        stub_task: task.Task = get_stub_task()
        stub_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task])

        dag_definition: str = compiler.compile(stub_pipeline)
        dag: models.DAG = get_dag_from_string(
            dag_id=stub_pipeline.name, dag_definition=dag_definition
        )

        assert len(dag.task_ids) == 1
        assert dag.task_ids == [stub_task.name]

    @pytest.mark.slow
    def test_can_compile_multiple_tasks(
        self,
        get_stub_tasks: Callable[..., list[task.Task]],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
        get_dag_from_string: Callable[..., models.DAG],
    ) -> None:
        compiler = airflow.Airflow()
        stub_task, stub_task_2 = get_stub_tasks(n=2, connected=True)
        stub_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task, stub_task_2])

        dag_definition: str = compiler.compile(stub_pipeline)
        dag: models.DAG = get_dag_from_string(
            dag_id=stub_pipeline.name, dag_definition=dag_definition
        )

        assert len(dag.task_ids) == 2
        assert set(dag.task_ids) == {stub_task.name, stub_task_2.name}
        assert get_dag_edges(dag=dag) == {(stub_task.name, stub_task_2.name)}

    @pytest.mark.xfail
    @pytest.mark.slow
    def test_can_decompile_single_task(
        self,
        get_stub_task: Callable[..., task.Task],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
    ) -> None:
        os.environ['ACTION_PATH'] = str(object=pathlib.Path(__file__).parent)
        compiler = airflow.Airflow()
        stub_task: task.Task = get_stub_task()
        expected_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task])
        dag_definition: str = compiler.compile(expected_pipeline)

        actual_pipeline: port.Port = compiler.decompile(
            artifact=dag_definition, object=pipeline.Pipeline
        )

        assert actual_pipeline == expected_pipeline

    @pytest.mark.xfail
    @pytest.mark.slow
    def test_can_decompile_multiple_tasks(
        self,
        get_stub_tasks: Callable[..., list[task.Task]],
        get_stub_pipeline: Callable[..., pipeline.Pipeline],
    ) -> None:
        compiler = airflow.Airflow()
        stub_task, stub_task_2 = get_stub_tasks(n=2, connected=True)
        expected_pipeline: pipeline.Pipeline = get_stub_pipeline(tasks=[stub_task, stub_task_2])
        dag_definition: str = compiler.compile(expected_pipeline)

        actual_pipeline: port.Port = compiler.decompile(
            artifact=dag_definition, object=pipeline.Pipeline
        )

        assert actual_pipeline == expected_pipeline
