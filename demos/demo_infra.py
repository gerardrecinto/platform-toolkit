#!/usr/bin/env python3
"""
Demonstrates the infra module: layered config, drift detection, and state management.
"""

import copy

from infra import Config, ConfigLayer, DriftDetector, StateStore


def demo_config():
    print("=" * 60)
    print("Config — layered, deep-merge with diff generation")
    print("=" * 60)

    defaults = ConfigLayer("defaults", {
        "replicas": 2,
        "image": {"repo": "registry.io/app", "tag": "latest"},
        "resources": {"cpu": "250m", "memory": "256Mi"},
        "env": {"LOG_LEVEL": "info", "TIMEOUT": "30"},
    }, priority=0)

    staging = ConfigLayer("staging", {
        "replicas": 3,
        "image": {"tag": "v2.1.0"},
        "env": {"LOG_LEVEL": "debug"},
    }, priority=10)

    prod_override = ConfigLayer("prod", {
        "replicas": 8,
        "image": {"tag": "v2.1.0"},
        "resources": {"cpu": "1000m", "memory": "1Gi"},
    }, priority=20)

    # Layered resolution
    staging_cfg = Config()
    staging_cfg.push(defaults).push(staging)

    prod_cfg = Config()
    prod_cfg.push(defaults).push(prod_override)

    print(f"\nStaging config: {staging_cfg}")
    print(f"  replicas : {staging_cfg['replicas']}")
    print(f"  log level: {staging_cfg['env']['LOG_LEVEL']}")
    print(f"  image tag: {staging_cfg['image']['tag']}")

    print(f"\nProd config: {prod_cfg}")
    print(f"  replicas : {prod_cfg['replicas']}")
    print(f"  cpu      : {prod_cfg['resources']['cpu']}")

    # Diff between staging and prod
    print("\nDiff staging → prod:")
    for key, staging_val, prod_val in staging_cfg.diff(prod_cfg):
        print(f"  {key:12s}: {staging_val!r:30s} → {prod_val!r}")

    # Fork with runtime override
    canary = prod_cfg.fork({"replicas": 1, "canary": True})
    print(f"\nCanary fork replicas: {canary['replicas']}")
    print(f"Prod still has:       {prod_cfg['replicas']}")

    # Snapshot for rollback testing
    snap = prod_cfg.snapshot()
    snap["replicas"] = 99
    print(f"\nSnapshot mutated to 99 — original unchanged: {prod_cfg['replicas']}")

    # Shallow vs deep clone of ConfigLayer
    layer_shallow = defaults.shallow_clone()
    layer_deep = defaults.deep_clone()
    layer_shallow["replicas"] = 999
    print(f"\nShallow clone mutated replicas=999")
    print(f"  Original defaults replicas: {defaults['replicas']}")
    print(f"  Deep clone replicas: {layer_deep['replicas']}")


def demo_drift():
    print("\n" + "=" * 60)
    print("DriftDetector — recursive generator with yield from")
    print("=" * 60)

    desired = {
        "deployment": {
            "replicas": 8,
            "image": "registry.io/app:v2.1.0",
            "resources": {"cpu": "1000m", "memory": "1Gi"},
        },
        "hpa": {
            "minReplicas": 4,
            "maxReplicas": 20,
            "targetCPU": 60,
        },
        "service": {"port": 8080, "type": "ClusterIP"},
    }

    actual = {
        "deployment": {
            "replicas": 3,           # drifted
            "image": "registry.io/app:v2.0.9",  # drifted
            "resources": {"cpu": "1000m", "memory": "1Gi"},
        },
        "hpa": {
            "minReplicas": 4,
            "maxReplicas": 20,
            "targetCPU": 80,         # drifted
        },
        "service": {"port": 8080, "type": "ClusterIP"},
        "debug_pod": {"enabled": True},  # added in actual
    }

    detector = DriftDetector.for_kubernetes()

    print("\nAll drifted keys:")
    for result in detector.drifted_only(desired, actual):
        print(f"  {result}")

    summary = detector.summary(desired, actual)
    print(f"\nDrift summary: {summary}")

    # Flatten nested structure
    print("\nFlattened desired config (dotted paths):")
    for path, value in DriftDetector.flatten(desired):
        print(f"  {path:45s} = {value!r}")

    # Patch back to desired
    drifted = list(detector.drifted_only(desired, actual))
    patched = DriftDetector.patch(actual, drifted)
    print(f"\nAfter patch — deployment.replicas: {patched['deployment']['replicas']}")
    print(f"Actual unchanged:                  {actual['deployment']['replicas']}")


def demo_state():
    print("\n" + "=" * 60)
    print("StateStore — snapshot history, generators, descriptors")
    print("=" * 60)

    store = StateStore("pipeline-state")
    store.update({
        "status": "pending",
        "job_count": 7,
        "failed_jobs": [],
        "metadata": {"branch": "main", "sha": "abc123"},
    })
    store.take_snapshot("initial")

    store.set("status", "running")
    store.set("current_job", "build")
    store.take_snapshot("running")

    store.set("status", "failed")
    store.update({"failed_jobs": ["unit-tests"], "current_job": None})
    store.take_snapshot("post-failure")

    print(f"\nStore: {store}")
    print(f"Version: {store.version}")

    print("\nSnapshot history:")
    for snap in store.history():
        print(f"  {snap.label:20s}  keys={list(snap.data)}")

    print("\nChanges since 'running' snapshot:")
    for key, val in store.changes_since("running"):
        print(f"  {key}: {val!r}")

    # Fork (shallow) vs clone (deep)
    fork = store.fork("fork-for-retry")
    fork.set("status", "retrying")

    cloned = store.clone("clone-for-audit")
    cloned.set("status", "audit")

    print(f"\nOriginal status: {store.get('status')!r}")
    print(f"Fork status:     {fork.get('status')!r}")
    print(f"Clone status:    {cloned.get('status')!r}")

    # Restore from snapshot
    store.restore("running")
    print(f"\nRestored to 'running' — status: {store.get('status')!r}")

    # Merge two stores
    extra = StateStore("metrics")
    extra.update({"job_duration": 42.1, "queue_depth": 3})
    merged = StateStore.merge(store, extra, name="merged")
    print(f"\nMerged store keys: {list(merged)}")

    # Descriptor validation
    print(f"\nDescriptor-enforced version field: {store.version}")
    try:
        store.version = "bad"  # type: ignore
    except TypeError as e:
        print(f"Type enforcement: {e}")


if __name__ == "__main__":
    demo_config()
    demo_drift()
    demo_state()
