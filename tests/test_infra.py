"""Tests for infra module"""

import copy
import pytest

from infra.config import Config, ConfigLayer
from infra.drift import DriftDetector, DriftType, DriftResult
from infra.state import StateStore, Snapshot


# ------------------------------------------------------------------ #
# Config tests                                                         #
# ------------------------------------------------------------------ #

def test_config_layer_resolution():
    cfg = Config.layered(
        ("defaults", {"a": 1, "b": 2}, 0),
        ("override", {"b": 99}, 10),
    )
    assert cfg["a"] == 1
    assert cfg["b"] == 99


def test_config_missing_key():
    cfg = Config.from_dict({"x": 1})
    with pytest.raises(KeyError):
        _ = cfg["missing"]


def test_config_diff():
    a = Config.from_dict({"x": 1, "y": 2, "z": 3})
    b = Config.from_dict({"x": 1, "y": 99, "w": 4})
    diffs = list(a.diff(b))
    keys = {d[0] for d in diffs}
    assert "y" in keys
    assert "z" in keys
    assert "w" in keys
    assert "x" not in keys


def test_config_fork_isolation():
    cfg = Config.from_dict({"replicas": 4})
    fork = cfg.fork({"replicas": 1})
    assert fork["replicas"] == 1
    assert cfg["replicas"] == 4


def test_config_snapshot_isolation():
    cfg = Config.from_dict({"version": "v1"})
    snap = cfg.snapshot()
    snap["version"] = "v2"
    assert cfg["version"] == "v1"


def test_config_validate_keys():
    cfg = Config.from_dict({"a": 1})
    missing = Config.validate_keys(cfg, ["a", "b", "c"])
    assert set(missing) == {"b", "c"}


def test_config_layer_shallow_clone_shares_refs():
    layer = ConfigLayer("base", {"nested": {"x": 1}})
    cloned = layer.shallow_clone()
    # Shallow copy: nested dict is shared
    assert layer._data["nested"] is cloned._data["nested"]


def test_config_layer_deep_clone_independent():
    layer = ConfigLayer("base", {"nested": {"x": 1}})
    cloned = layer.deep_clone()
    cloned["nested"]["x"] = 999
    assert layer["nested"]["x"] == 1


# ------------------------------------------------------------------ #
# DriftDetector tests                                                  #
# ------------------------------------------------------------------ #

def test_drift_added():
    detector = DriftDetector()
    desired = {"a": 1}
    actual = {"a": 1, "b": 2}
    results = list(detector.drifted_only(desired, actual))
    assert any(r.path == "b" and r.drift_type == DriftType.ADDED for r in results)


def test_drift_removed():
    detector = DriftDetector()
    desired = {"a": 1, "b": 2}
    actual = {"a": 1}
    results = list(detector.drifted_only(desired, actual))
    assert any(r.path == "b" and r.drift_type == DriftType.REMOVED for r in results)


def test_drift_changed():
    detector = DriftDetector()
    desired = {"replicas": 8}
    actual = {"replicas": 3}
    results = list(detector.drifted_only(desired, actual))
    assert results[0].drift_type == DriftType.CHANGED
    assert results[0].desired == 8
    assert results[0].actual == 3


def test_drift_nested():
    detector = DriftDetector()
    desired = {"resources": {"cpu": "1000m", "memory": "1Gi"}}
    actual = {"resources": {"cpu": "250m", "memory": "1Gi"}}
    results = list(detector.drifted_only(desired, actual))
    assert any("cpu" in r.path for r in results)


def test_drift_ignored_keys():
    detector = DriftDetector(ignore_keys={"uid", "resourceVersion"})
    desired = {"uid": "old", "name": "app"}
    actual = {"uid": "new", "name": "app"}
    results = list(detector.drifted_only(desired, actual))
    assert not any(r.path == "uid" for r in results)


def test_drift_summary():
    detector = DriftDetector()
    desired = {"a": 1, "b": 2}
    actual = {"a": 99, "c": 3}
    summary = detector.summary(desired, actual)
    assert summary["CHANGED"] >= 1
    assert summary["REMOVED"] >= 1
    assert summary["ADDED"] >= 1


def test_drift_flatten():
    nested = {"a": {"b": {"c": 42}}, "x": 1}
    flat = dict(DriftDetector.flatten(nested))
    assert flat["a.b.c"] == 42
    assert flat["x"] == 1


def test_drift_patch():
    detector = DriftDetector()
    desired = {"replicas": 8}
    actual = {"replicas": 3}
    drifted = list(detector.drifted_only(desired, actual))
    patched = DriftDetector.patch(actual, drifted)
    assert patched["replicas"] == 8
    assert actual["replicas"] == 3  # original unchanged


# ------------------------------------------------------------------ #
# StateStore tests                                                     #
# ------------------------------------------------------------------ #

def test_state_set_get():
    store = StateStore("test")
    store.set("key", "value")
    assert store.get("key") == "value"


def test_state_get_returns_deep_copy():
    store = StateStore("test")
    store.set("data", {"x": 1})
    val = store.get("data")
    val["x"] = 999
    assert store.get("data")["x"] == 1


def test_state_snapshot_and_restore():
    store = StateStore("test")
    store.set("status", "pending")
    store.take_snapshot("s1")
    store.set("status", "running")
    store.restore("s1")
    assert store.get("status") == "pending"


def test_state_changes_since():
    store = StateStore("test")
    store.update({"a": 1, "b": 2})
    store.take_snapshot("base")
    store.set("a", 99)
    store.set("c", 3)
    changes = dict(store.changes_since("base"))
    assert changes["a"] == 99
    assert "c" in changes
    assert "b" not in changes


def test_state_fork_isolation():
    store = StateStore("main")
    store.set("x", 1)
    fork = store.fork()
    fork.set("x", 999)
    assert store.get("x") == 1


def test_state_clone_independence():
    store = StateStore("main")
    store.update({"nested": {"deep": 1}})
    cloned = store.clone()
    cloned.set("nested", {"deep": 999})
    assert store.get("nested")["deep"] == 1


def test_state_merge():
    s1 = StateStore("a")
    s1.set("x", 1)
    s2 = StateStore("b")
    s2.set("y", 2)
    merged = StateStore.merge(s1, s2)
    assert merged.get("x") == 1
    assert merged.get("y") == 2


def test_descriptor_type_enforcement():
    store = StateStore("test")
    store.version = 5
    with pytest.raises(TypeError):
        store.version = "bad"  # type: ignore


def test_state_iter():
    store = StateStore("test")
    store.update({"a": 1, "b": 2, "c": 3})
    keys = set(store)
    assert keys == {"a", "b", "c"}
