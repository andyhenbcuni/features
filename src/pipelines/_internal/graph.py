"Simplified wrapper of nx.DiGraph"

import itertools
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar

import networkx as nx


class Node(Protocol):
    "Protocol for NodeTypes"

    name: str
    depends_on: list[str]


NodeType = TypeVar('NodeType', bound=Node)


@dataclass
class DAG(Generic[NodeType]):
    """DAG Class."""

    nodes: list[NodeType]
    _graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    def __getitem__(self, key: str) -> NodeType:
        return self.node_map[key]

    def __iter__(self) -> Iterator[NodeType]:
        for node in nx.topological_sort(G=self.graph):
            yield self.node_map[node]

    @property
    def graph(self) -> nx.DiGraph:
        """Internal networkx DiGraph."""

        for node in self.nodes:
            self._graph.add_node(node_for_adding=node.name)
            if node.depends_on:
                self._graph.add_edges_from(
                    ebunch_to_add=set(zip(node.depends_on, itertools.repeat(object=node.name)))
                )
        return self._graph

    @property
    def node_map(self) -> dict[str, NodeType]:
        return {node.name: node for node in self.nodes}

    @property
    def roots(self) -> list[str]:
        """Gets roots of DAG."""
        return [node for node in self.graph.nodes() if self.graph.in_degree(nbunch=node) == 0]

    @property
    def leaves(self) -> list[str]:
        """Gets leaf nodes of DAG."""
        return [node for node in self.graph.nodes() if self.graph.out_degree(nbunch=node) == 0]
