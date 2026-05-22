"""Tests for pipeline.dag"""

import copy
import pytest

from pipeline.dag import DAG, DagNode


SIMPLE_PIPELINE = {
    "a": {"command": "echo a", "depends_on": []},
    "b": {"command": "echo b", "depends_on": ["a"]},
    "c": {"command": "echo c", "depends_on": ["a"]},
    "d": {"command": "echo d", "depends_on": ["b", "c"]},
}


def test_topological_order():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    order = [n.name for n in dag]
    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")


def test_layers():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    layers = [sorted(n.name for n in layer) for layer in dag.layers()]
    assert layers[0] == ["a"]
    assert set(layers[1]) == {"b", "c"}
    assert layers[2] == ["d"]


def test_affected_by():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    affected = set(dag.affected_by("a"))
    assert "b" in affected
    assert "c" in affected
    assert "d" in affected
    assert "a" not in affected


def test_critical_path():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    path = dag.critical_path()
    assert path[0] == "a"
    assert path[-1] == "d"


def test_validate_no_cycles_clean():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    assert DAG.validate_no_cycles(dag._nodes)


def test_cycle_raises():
    cyclic = {
        "x": {"command": "x", "depends_on": ["z"]},
        "y": {"command": "y", "depends_on": ["x"]},
        "z": {"command": "z", "depends_on": ["y"]},
    }
    dag = DAG.from_dict(cyclic)
    with pytest.raises(RuntimeError, match="[Cc]ycle"):
        list(dag)


def test_contains():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    assert "a" in dag
    assert "z" not in dag


def test_len():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    assert len(dag) == 4


def test_merge():
    dag1 = DAG.from_dict({"x": {"command": "x", "depends_on": []}})
    dag2 = DAG.from_dict({"y": {"command": "y", "depends_on": []}})
    merged = DAG.merge(dag1, dag2)
    assert len(merged) == 2


def test_shallow_copy():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    dag2 = copy.copy(dag)
    assert len(dag2) == len(dag)
    # Shallow copy shares node references
    assert dag._nodes["a"] is dag2._nodes["a"]


def test_deep_copy():
    dag = DAG.from_dict(SIMPLE_PIPELINE)
    dag2 = copy.deepcopy(dag)
    assert len(dag2) == len(dag)
    # Deep copy produces independent nodes
    assert dag._nodes["a"] is not dag2._nodes["a"]
