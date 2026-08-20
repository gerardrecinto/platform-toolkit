"""Priority-based job scheduler with generator-driven queue."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Generator, Iterator


class Priority(IntEnum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass(order=True)
class _ScheduledJob:
    priority: int
    scheduled_at: float
    job_id: str = field(compare=False)
    command: str = field(compare=False)
    tags: frozenset[str] = field(default_factory=frozenset, compare=False)


class JobScheduler:
    """
    Min-heap job scheduler.  Yields runnable jobs in priority order via
    the iterator protocol; supports filtering by tag through generators.
    """

    def __init__(self, max_concurrent: int = 8) -> None:
        self._heap: list[_ScheduledJob] = []
        self._max_concurrent = max_concurrent
        self._running: set[str] = set()
        self._completed: list[str] = []

    # ------------------------------------------------------------------ #
    # Submission                                                           #
    # ------------------------------------------------------------------ #

    def submit(
        self,
        job_id: str,
        command: str,
        priority: Priority = Priority.NORMAL,
        tags: frozenset[str] | None = None,
        delay: float = 0.0,
    ) -> None:
        job = _ScheduledJob(
            priority=int(priority),
            scheduled_at=time.monotonic() + delay,
            job_id=job_id,
            command=command,
            tags=tags or frozenset(),
        )
        heapq.heappush(self._heap, job)

    def cancel(self, job_id: str) -> bool:
        before = len(self._heap)
        self._heap = [j for j in self._heap if j.job_id != job_id]
        heapq.heapify(self._heap)
        return len(self._heap) < before

    # ------------------------------------------------------------------ #
    # Generators                                                           #
    # ------------------------------------------------------------------ #

    def runnable(self, now: float | None = None) -> Generator[_ScheduledJob, None, None]:
        """
        Yield jobs whose scheduled_at has passed, in priority order, up to
        the available concurrency slots. Each yielded job is marked running;
        call mark_done() once it finishes to free its slot.
        """
        t = now if now is not None else time.monotonic()
        temp: list[_ScheduledJob] = []
        while self._heap and self._heap[0].scheduled_at <= t:
            job = heapq.heappop(self._heap)
            if len(self._running) < self._max_concurrent:
                self._running.add(job.job_id)
                yield job
            else:
                temp.append(job)
        for j in temp:
            heapq.heappush(self._heap, j)

    def mark_done(self, job_id: str) -> None:
        """Free the concurrency slot held by job_id and record it as completed."""
        self._running.discard(job_id)
        self._completed.append(job_id)

    def by_tag(self, tag: str) -> Generator[_ScheduledJob, None, None]:
        """Generator filter — yields only queued jobs with the given tag."""
        yield from (j for j in self._heap if tag in j.tags)

    def drain(self) -> Generator[_ScheduledJob, None, None]:
        """Yield and remove every job in the queue (used for shutdown)."""
        while self._heap:
            yield heapq.heappop(self._heap)

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_pipeline(cls, jobs: list[dict], max_concurrent: int = 8) -> "JobScheduler":
        """Build a scheduler from a serialised pipeline job list."""
        scheduler = cls(max_concurrent=max_concurrent)
        for spec in jobs:
            scheduler.submit(
                job_id=spec["id"],
                command=spec["command"],
                priority=Priority[spec.get("priority", "NORMAL").upper()],
                tags=frozenset(spec.get("tags", [])),
                delay=float(spec.get("delay", 0)),
            )
        return scheduler

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def estimate_wait(heap: list[_ScheduledJob], priority: Priority) -> float:
        """Estimate queue wait time for a new job at the given priority."""
        ahead = sum(1 for j in heap if j.priority <= int(priority))
        return ahead * 30.0  # rough 30s-per-job estimate

    @staticmethod
    def group_by_priority(jobs: list[_ScheduledJob]) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for job in jobs:
            label = Priority(job.priority).name
            groups.setdefault(label, []).append(job.job_id)
        return groups

    # ------------------------------------------------------------------ #
    # Iterator protocol                                                    #
    # ------------------------------------------------------------------ #

    def __iter__(self) -> Iterator[_ScheduledJob]:
        """Iterate a snapshot of the queue in priority order (non-destructive)."""
        return iter(sorted(self._heap))

    def __len__(self) -> int:
        return len(self._heap)

    def __repr__(self) -> str:
        return f"JobScheduler(queued={len(self._heap)}, running={len(self._running)})"
