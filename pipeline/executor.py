"""Async pipeline executor — runs DAG layers in parallel with result streaming."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import AsyncGenerator, Generator


class JobStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass(slots=True)
class JobResult:
    job_id: str
    status: JobStatus
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    duration: float = 0.0
    started_at: float = field(default_factory=time.time)

    @property
    def succeeded(self) -> bool:
        return self.status is JobStatus.SUCCESS

    @property
    def failed(self) -> bool:
        return self.status is JobStatus.FAILED


class ExecutionContext:
    """
    Tracks live execution state across concurrent jobs.
    Used as an async context manager to ensure cleanup on exit.
    """

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id = pipeline_id
        self.results: dict[str, JobResult] = {}
        self._start_time: float = 0.0

    async def __aenter__(self) -> "ExecutionContext":
        self._start_time = time.monotonic()
        return self

    async def __aexit__(self, *_) -> None:
        pass

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start_time

    def record(self, result: JobResult) -> None:
        self.results[result.job_id] = result

    def failed_jobs(self) -> Generator[str, None, None]:
        yield from (jid for jid, r in self.results.items() if r.failed)

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in self.results.values():
            counts[r.status.name] = counts.get(r.status.name, 0) + 1
        return counts


class JobExecutor:
    """
    Executes pipeline jobs concurrently, streaming results as they complete.

    Uses async generators so callers can react to each result immediately
    rather than waiting for the whole layer to finish.
    """

    def __init__(self, concurrency: int = 4, dry_run: bool = False) -> None:
        self._concurrency = concurrency
        self._dry_run = dry_run
        self._sem: asyncio.Semaphore | None = None

    @asynccontextmanager
    async def session(self, pipeline_id: str) -> AsyncGenerator[ExecutionContext, None]:
        self._sem = asyncio.Semaphore(self._concurrency)
        async with ExecutionContext(pipeline_id) as ctx:
            yield ctx
        self._sem = None

    async def run_layer(
        self, jobs: list[tuple[str, str]], ctx: ExecutionContext
    ) -> AsyncGenerator[JobResult, None]:
        """
        Async generator that runs a layer of jobs concurrently and yields
        each result as soon as it completes.
        """
        queue: asyncio.Queue[JobResult] = asyncio.Queue()

        async def _run_one(job_id: str, command: str) -> None:
            assert self._sem is not None
            async with self._sem:
                result = await self._execute(job_id, command)
            ctx.record(result)
            await queue.put(result)

        tasks = [asyncio.create_task(_run_one(jid, cmd)) for jid, cmd in jobs]

        for _ in jobs:
            yield await queue.get()

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute(self, job_id: str, command: str) -> JobResult:
        t0 = time.monotonic()
        if self._dry_run:
            await asyncio.sleep(0.01)
            return JobResult(
                job_id=job_id,
                status=JobStatus.SUCCESS,
                stdout=f"[dry-run] {command}",
                duration=time.monotonic() - t0,
            )
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            status = JobStatus.SUCCESS if proc.returncode == 0 else JobStatus.FAILED
            return JobResult(
                job_id=job_id,
                status=status,
                stdout=stdout.decode(errors="replace"),
                stderr=stderr.decode(errors="replace"),
                exit_code=proc.returncode or 0,
                duration=time.monotonic() - t0,
            )
        except asyncio.TimeoutError:
            return JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                stderr="job timed out",
                exit_code=124,
                duration=time.monotonic() - t0,
            )

    @staticmethod
    def format_summary(results: dict[str, JobResult]) -> str:
        lines = [f"  {jid:30s} {r.status.name:8s}  {r.duration:.2f}s" for jid, r in results.items()]
        return "\n".join(lines)

    @classmethod
    def dry_run(cls, concurrency: int = 4) -> "JobExecutor":
        """Factory that always returns a non-executing executor — good for CI plan step."""
        return cls(concurrency=concurrency, dry_run=True)
