import dataclasses

from src.pipelines._internal import graph


@dataclasses.dataclass
class StubNode:
    name: str
    depends_on: list[str]


class TestDAG:
    task_1 = StubNode(name='task_1', depends_on=[])
    task_2 = StubNode(name='task_2', depends_on=['task_1'])
    task_3 = StubNode(name='task_3', depends_on=['task_2'])
    nodes: list[StubNode] = [task_1, task_2, task_3]
    dag: graph.DAG[StubNode] = graph.DAG(nodes=nodes)

    def test_dag_iter_sorts_topologically(self) -> None:
        assert list(self.dag) == self.nodes

    def test_dag_roots(self) -> None:
        assert self.dag.roots == [self.task_1.name]

    def test_dag_leaves(self) -> None:
        assert self.dag.leaves == [self.task_3.name]

    def test_node_map(self) -> None:
        assert self.dag.node_map == {task.name: task for task in self.nodes}  # type: ignore[reportPrivateUsage]
