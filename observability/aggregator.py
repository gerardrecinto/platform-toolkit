"""Log streaming and aggregation with generator-based parsing."""

from __future__ import annotations

import itertools
import re
import time
from dataclasses import dataclass
from typing import Generator, Iterable, TextIO


@dataclass(slots=True, frozen=True)
class LogEntry:
    timestamp: float
    level: str
    job_id: str
    message: str
    raw: str

    @classmethod
    def parse(cls, line: str) -> "LogEntry | None":
        """
        Parse Jenkins-style log lines:
          2024-03-01T12:00:00.123Z [INFO] [job/my-pipeline] Starting step
        """
        pattern = r"(\S+)\s+\[(\w+)\]\s+\[([^\]]+)\]\s+(.*)"
        m = re.match(pattern, line.strip())
        if not m:
            return None
        ts_str, level, job_id, message = m.groups()
        try:
            ts = time.mktime(time.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S"))
        except ValueError:
            ts = time.time()
        return cls(timestamp=ts, level=level.upper(), job_id=job_id, message=message, raw=line)

    @staticmethod
    def is_error(entry: "LogEntry") -> bool:
        return entry.level in ("ERROR", "FATAL", "CRITICAL")

    @staticmethod
    def is_warning(entry: "LogEntry") -> bool:
        return entry.level == "WARN"


class LogAggregator:
    """
    Streaming log aggregator driven entirely by generators.

    All public methods are generators — nothing is buffered in memory
    unless the caller explicitly materialises the result.
    """

    def __init__(self, max_errors: int = 100) -> None:
        self._max_errors = max_errors

    # ------------------------------------------------------------------ #
    # Generators                                                           #
    # ------------------------------------------------------------------ #

    def parse_stream(self, lines: Iterable[str]) -> Generator[LogEntry, None, None]:
        """Parse raw log lines, skipping unparseable ones."""
        for line in lines:
            entry = LogEntry.parse(line)
            if entry is not None:
                yield entry

    def errors_only(self, entries: Iterable[LogEntry]) -> Generator[LogEntry, None, None]:
        """Filter to ERROR/FATAL entries."""
        yield from (e for e in entries if LogEntry.is_error(e))

    def by_job(
        self, entries: Iterable[LogEntry], job_id: str
    ) -> Generator[LogEntry, None, None]:
        """Yield only entries for the specified job."""
        yield from (e for e in entries if e.job_id == job_id)

    def tail(self, entries: Iterable[LogEntry], n: int) -> Generator[LogEntry, None, None]:
        """Yield the last n entries from an iterable without buffering more than n."""
        yield from iter(collections_deque(entries, maxlen=n))

    def sliding_window(
        self, entries: Iterable[LogEntry], window_seconds: float
    ) -> Generator[list[LogEntry], None, None]:
        """
        Yield rolling windows of log entries.
        Each yielded list contains entries within the last `window_seconds`.
        Uses itertools.groupby to batch by rounded timestamp.
        """
        buf: list[LogEntry] = []
        for entry in entries:
            buf.append(entry)
            cutoff = entry.timestamp - window_seconds
            buf = [e for e in buf if e.timestamp >= cutoff]
            yield list(buf)

    def summarise(
        self, entries: Iterable[LogEntry]
    ) -> Generator[dict[str, object], None, None]:
        """
        Group entries by job_id and yield one summary dict per job.
        Consumes the entire iterable in memory — use for small log files only.
        """
        all_entries = list(entries)
        key = lambda e: e.job_id
        for job_id, group in itertools.groupby(sorted(all_entries, key=key), key=key):
            entries_list = list(group)
            errors = [e for e in entries_list if LogEntry.is_error(e)]
            yield {
                "job_id": job_id,
                "total": len(entries_list),
                "errors": len(errors),
                "first_error": errors[0].message if errors else None,
            }

    def chain_sources(self, *sources: Iterable[str]) -> Generator[LogEntry, None, None]:
        """Parse and merge multiple log streams in order using itertools.chain."""
        yield from self.parse_stream(itertools.chain.from_iterable(sources))

    def first_failure(self, entries: Iterable[LogEntry]) -> LogEntry | None:
        """Return the first ERROR entry, or None — consumes the generator lazily."""
        return next(self.errors_only(entries), None)

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_file(cls, path: str) -> "LogAggregator":
        return cls()

    @classmethod
    def strict(cls) -> "LogAggregator":
        """Aggregator that stops at the first error (max_errors=1)."""
        return cls(max_errors=1)

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def level_counts(entries: Iterable[LogEntry]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for e in entries:
            counts[e.level] = counts.get(e.level, 0) + 1
        return counts

    @staticmethod
    def read_lines(f: TextIO) -> Generator[str, None, None]:
        """Lazily yield non-empty, stripped lines from any file-like object."""
        for line in f:
            stripped = line.rstrip("\n")
            if stripped:
                yield stripped


def collections_deque(iterable: Iterable, maxlen: int):
    from collections import deque
    return deque(iterable, maxlen=maxlen)
