"""Directed acyclic graph for CI/CD job dependencies."""

from __future__ import annotations

import copy
from collections import deque
from dataclasses import dataclass, field
from typing import Generator, Iterator, Self


@dataclass(slots=True)
class DagNode:
    """Single node in the pipeline graph."""

    name: str
    command: str
    depends_on: list[str] = field(default_factory=list)
    timeout: int = 300
    retries: int = 0
    tags: frozenset[str] = field(default_factory=frozenset)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DagNode) and self.name == other.name


class _TopologicalIterator:
    """Kahn's algorithm iterator — yields nodes in dependency order."""

    def __init__(self, adjacency: dict[str, list[str]], nodes: dict[str, DagNode]) -> None:
        self._nodes = nodes
        self._in_degree: dict[str, int] = {n: 0 for n in adjacency}
        self._adj = adjacency

        for deps in adjacency.values():
            for dep in deps:
                self._in_degree[dep] = self._in_degree.get(dep, 0) + 1

        self._queue: deque[str] = deque(
            name for name, deg in self._in_degree.items() if deg == 0
        )
        self._visited = 0

    def __iter__(self) -> Iterator[DagNode]:
        return self

    def __next__(self) -> DagNode:
        if not self._queue:
            if self._visited < len(self._nodes):
                raise RuntimeError("Cycle detected in pipeline graph")
            raise StopIteration

        name = self._queue.popleft()
        self._visited += 1

        for dependent in self._adj.get(name, []):
            self._in_degree[dependent] -= 1
            if self._in_degree[dependent] == 0:
                self._queue.append(dependent)

        return self._nodes[name]


class DAG:
    """
    Pipeline DAG — resolves job execution order, detects cycles,
    and exposes lazy traversal via the iterator protocol.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, DagNode] = {}
        # adjacency maps each node to the nodes that depend on it
        self._dependents: dict[str, list[str]] = {}

    # ------------------------------------------------------------------ #
    # Mutation                                                             #
    # ------------------------------------------------------------------ #

    def add(self, node: DagNode) -> Self:
        self._nodes[node.name] = node
        self._dependents.setdefault(node.name, [])
        for dep in node.depends_on:
            self._dependents.setdefault(dep, []).append(node.name)
        return self

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_dict(cls, definition: dict[str, dict]) -> "DAG":
        """Build a DAG from a plain dict (e.g., parsed YAML pipeline file)."""
        dag = cls()
        for name, spec in definition.items():
            dag.add(
                DagNode(
                    name=name,
                    command=spec.get("command", ""),
                    depends_on=spec.get("depends_on", []),
                    timeout=spec.get("timeout", 300),
                    retries=spec.get("retries", 0),
                    tags=frozenset(spec.get("tags", [])),
                )
            )
        return dag

    @classmethod
    def merge(cls, *dags: "DAG") -> "DAG":
        """Combine multiple DAGs into one — useful for multi-repo pipelines."""
        merged = cls()
        for dag in dags:
            for node in dag._nodes.values():
                merged.add(copy.copy(node))
        return merged

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def validate_no_cycles(nodes: dict[str, DagNode]) -> bool:
        """Return True when no cyclic dependencies exist."""
        visited: set[str] = set()
        in_stack: set[str] = set()

        def dfs(name: str) -> bool:
            visited.add(name)
            in_stack.add(name)
            for dep in nodes.get(name, DagNode(name, "")).depends_on:
                if dep not in visited:
                    if not dfs(dep):
                        return False
                elif dep in in_stack:
                    return False
            in_stack.discard(name)
            return True

        return all(dfs(n) for n in nodes if n not in visited)

    @staticmethod
    def _build_reverse(nodes: dict[str, DagNode]) -> dict[str, list[str]]:
        rev: dict[str, list[str]] = {n: [] for n in nodes}
        for name, node in nodes.items():
            for dep in node.depends_on:
                rev.setdefault(dep, []).append(name)
        return rev

    # ------------------------------------------------------------------ #
    # Iterator / generator interface                                       #
    # ------------------------------------------------------------------ #

    def __iter__(self) -> _TopologicalIterator:
        """Iterate nodes in topological (dependency-safe) order."""
        return _TopologicalIterator(self._dependents, self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, name: str) -> bool:
        return name in self._nodes

    def layers(self) -> Generator[list[DagNode], None, None]:
        """
        Yield batches of nodes that can run in parallel.
        Each yielded list has no intra-layer dependencies.
        """
        in_degree = {
            name: len(node.depends_on) for name, node in self._nodes.items()
        }
        pending = set(self._nodes)

        while pending:
            layer = [self._nodes[n] for n in pending if in_degree[n] == 0]
            if not layer:
                raise RuntimeError("Cycle prevents further layer resolution")
            yield layer
            for node in layer:
                pending.discard(node.name)
                for dep in self._dependents.get(node.name, []):
                    in_degree[dep] -= 1

    def affected_by(self, changed: str) -> Generator[str, None, None]:
        """Yield all downstream job names transitively affected by a change."""
        rev = self._build_reverse(self._nodes)
        seen: set[str] = set()
        queue = deque(rev.get(changed, []))
        while queue:
            name = queue.popleft()
            if name in seen:
                continue
            seen.add(name)
            yield name
            queue.extend(rev.get(name, []))

    def critical_path(self) -> list[str]:
        """Return the longest dependency chain by node count."""
        memo: dict[str, int] = {}

        def depth(name: str) -> int:
            if name in memo:
                return memo[name]
            node = self._nodes[name]
            result = 1 + max((depth(d) for d in node.depends_on), default=0)
            memo[name] = result
            return result

        if not self._nodes:
            return []

        end = max(self._nodes, key=depth)
        path: list[str] = []
        cur = end
        while cur:
            path.append(cur)
            deps = self._nodes[cur].depends_on
            cur = max(deps, key=depth) if deps else ""
        path.reverse()
        return path

    # ------------------------------------------------------------------ #
    # Copy semantics                                                       #
    # ------------------------------------------------------------------ #

    def __copy__(self) -> "DAG":
        """Shallow copy shares DagNode references — fast for read-only forks."""
        new = DAG()
        new._nodes = dict(self._nodes)
        new._dependents = {k: list(v) for k, v in self._dependents.items()}
        return new

    def __deepcopy__(self, memo: dict) -> "DAG":
        """Deep copy produces a fully independent pipeline graph."""
        new = DAG()
        new._nodes = {k: copy.deepcopy(v, memo) for k, v in self._nodes.items()}
        new._dependents = copy.deepcopy(self._dependents, memo)
        return new

    def __repr__(self) -> str:
        return f"DAG(nodes={list(self._nodes)})"
