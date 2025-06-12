import itertools
import pathlib
from collections.abc import Iterable

from src.pipelines import pipeline, port, utils
from src.pipelines._internal import graph

TEMPLATE_PATH: pathlib.Path = pathlib.Path(__file__).parent / 'templates' / 'mermaid.jinja2'

# TODO: add action to mermaid to decompile


class Mermaid:
    def compile(self, object: object) -> str:
        if isinstance(object, pipeline.Pipeline):
            dag = graph.DAG(nodes=object.tasks)
            all_nodes: set[str] = set(dag.graph.nodes)
            edges: Iterable[str] = dag.graph.edges
            connected_nodes: set[str] = set(itertools.chain.from_iterable(edges))
            return utils.read_template(
                template_path=TEMPLATE_PATH,
                template_fields={
                    'name': object.name,
                    'edges': sorted(edges),
                    'unconnected_nodes': sorted(
                        all_nodes - connected_nodes,
                    ),
                },
            )
        else:
            raise NotImplementedError()

    @classmethod
    def decompile(cls, artifact: str, object: type) -> port.Port:
        raise NotImplementedError('Cannot decompile from mermaid.')
