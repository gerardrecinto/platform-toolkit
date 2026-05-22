#!/usr/bin/env python3
"""
Demonstrates the pipeline module: DAG construction, layer-based parallel execution,
artifact caching, and job scheduling.
"""

import asyncio
import copy
import time

from pipeline import DAG, DagNode, ArtifactCache, Artifact, JobScheduler, Priority


def demo_dag():
    print("=" * 60)
    print("DAG — topological traversal and parallel layers")
    print("=" * 60)

    pipeline = {
        "checkout":   {"command": "git checkout main", "depends_on": []},
        "lint":       {"command": "ruff check .", "depends_on": ["checkout"]},
        "unit-tests": {"command": "pytest tests/unit", "depends_on": ["checkout"]},
        "build":      {"command": "docker build .", "depends_on": ["lint", "unit-tests"]},
        "push":       {"command": "docker push registry/app", "depends_on": ["build"]},
        "deploy-dev": {"command": "kubectl apply -f k8s/dev", "depends_on": ["push"]},
        "smoke":      {"command": "pytest tests/smoke", "depends_on": ["deploy-dev"]},
    }

    dag = DAG.from_dict(pipeline)
    print(f"\nPipeline DAG: {len(dag)} jobs")

    print("\nTopological order:")
    for node in dag:
        deps = f"  ← {node.depends_on}" if node.depends_on else ""
        print(f"  {node.name}{deps}")

    print("\nParallel execution layers:")
    for i, layer in enumerate(dag.layers()):
        names = [n.name for n in layer]
        print(f"  Layer {i}: {names}")

    print(f"\nJobs affected by 'unit-tests' change:")
    for name in dag.affected_by("unit-tests"):
        print(f"  {name}")

    print(f"\nCritical path: {' → '.join(dag.critical_path())}")

    # Demonstrate deepcopy vs shallow copy
    dag_shallow = copy.copy(dag)
    dag_deep = copy.deepcopy(dag)
    print(f"\nShallow copy: {dag_shallow}")
    print(f"Deep copy:    {dag_deep}")


def demo_artifact_cache():
    print("\n" + "=" * 60)
    print("ArtifactCache — versioned caching with copy semantics")
    print("=" * 60)

    cache = ArtifactCache(max_entries=10)

    # Batch-write via context manager
    with cache.transaction() as staging:
        staging.append(Artifact.from_text("requirements.txt", "flask==3.0\npytest==8.0", tag="deps"))
        staging.append(Artifact.from_text("app.py", "from flask import Flask", tag="source"))
        staging.append(Artifact.from_bytes("binary", b"\x7fELF", tag="build"))

    print(f"\nCache state: {cache}")

    # Iterate oldest-to-newest
    print("\nCached artifacts (oldest first):")
    for artifact in cache:
        print(f"  {artifact.key:20s}  sha={artifact.version}  stale={artifact.is_stale(3600)}")

    # Fork (shallow copy) vs snapshot (deep copy)
    forked = cache.fork()
    snapped = cache.snapshot()
    forked.invalidate("app.py")

    print(f"\nOriginal: {len(cache)} entries")
    print(f"Fork (after invalidate): {len(forked)} entries")
    print(f"Snapshot (independent):  {len(snapped)} entries")
    print(f"Hit rate: {cache.hit_rate:.0%}")


def demo_scheduler():
    print("\n" + "=" * 60)
    print("JobScheduler — priority queue with generator drain")
    print("=" * 60)

    spec = [
        {"id": "build-main",    "command": "make build",    "priority": "HIGH"},
        {"id": "lint-pr-123",   "command": "ruff check .",  "priority": "NORMAL"},
        {"id": "scan-cve",      "command": "trivy image",   "priority": "CRITICAL"},
        {"id": "docs-build",    "command": "mkdocs build",  "priority": "LOW"},
        {"id": "perf-bench",    "command": "pytest bench/", "priority": "BACKGROUND"},
    ]

    scheduler = JobScheduler.from_pipeline(spec)
    print(f"\nQueued: {len(scheduler)} jobs")

    print("\nAll jobs in priority order:")
    for job in scheduler:
        print(f"  [{Priority(job.priority).name:10s}] {job.job_id}")

    print("\nRunnable jobs (next tick):")
    for job in scheduler.runnable(now=time.monotonic() + 1.0):
        wait = JobScheduler.estimate_wait(list(scheduler), Priority(job.priority))
        print(f"  {job.job_id:20s}  estimated wait: {wait:.0f}s")


async def demo_executor():
    print("\n" + "=" * 60)
    print("JobExecutor — async dry-run with streaming results")
    print("=" * 60)

    from pipeline import JobExecutor

    executor = JobExecutor.dry_run(concurrency=3)

    jobs = [
        ("checkout",   "git checkout main"),
        ("lint",       "ruff check ."),
        ("unit-tests", "pytest tests/"),
    ]

    async with executor.session("demo-pipeline") as ctx:
        print(f"\nRunning layer with {len(jobs)} jobs (concurrency=3):")
        async for result in executor.run_layer(jobs, ctx):
            print(f"  ✓ {result.job_id:20s}  {result.status.name}  {result.duration:.3f}s")

        print(f"\nSummary: {ctx.summary()}")
        print(f"Elapsed: {ctx.elapsed:.3f}s")


if __name__ == "__main__":
    demo_dag()
    demo_artifact_cache()
    demo_scheduler()
    asyncio.run(demo_executor())
