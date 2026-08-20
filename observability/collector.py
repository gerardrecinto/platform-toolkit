"""Metrics collection with a sliding-window iterable and Protocol-backed storage."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Iterator, Protocol, runtime_checkable


@dataclass(slots=True, frozen=True)
class Metric:
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: dict[str, str] = field(default_factory=dict)

    def with_label(self, key: str, value: str) -> "Metric":
        new_labels = {**self.labels, key: value}
        # frozen dataclass — return a new instance
        return Metric(
            name=self.name,
            value=self.value,
            timestamp=self.timestamp,
            labels=new_labels,
        )

    @staticmethod
    def gauge(name: str, value: float, **labels: str) -> "Metric":
        return Metric(name=name, value=value, labels=dict(labels))

    @staticmethod
    def counter(name: str, increment: float = 1.0, **labels: str) -> "Metric":
        return Metric(name=f"{name}_total", value=increment, labels=dict(labels))


@runtime_checkable
class MetricBackend(Protocol):
    """Any object implementing push + query satisfies this protocol."""

    def push(self, metric: Metric) -> None: ...
    def query(self, name: str, window: float) -> list[Metric]: ...


class MetricSeries:
    """
    Time-windowed ring buffer for a single metric name.
    Implements the iterable protocol — `for m in series` iterates
    entries oldest-to-newest within the retention window.
    """

    def __init__(self, name: str, retention: float = 300.0, max_samples: int = 10_000) -> None:
        self.name = name
        self._retention = retention
        self._buf: deque[Metric] = deque(maxlen=max_samples)

    def record(self, value: float, **labels: str) -> None:
        self._buf.append(Metric.gauge(self.name, value, **labels))

    def _trim(self) -> None:
        cutoff = time.time() - self._retention
        while self._buf and self._buf[0].timestamp < cutoff:
            self._buf.popleft()

    # ------------------------------------------------------------------ #
    # Iterable protocol                                                    #
    # ------------------------------------------------------------------ #

    def __iter__(self) -> Iterator[Metric]:
        self._trim()
        return iter(list(self._buf))

    def __len__(self) -> int:
        self._trim()
        return len(self._buf)

    # ------------------------------------------------------------------ #
    # Aggregations                                                         #
    # ------------------------------------------------------------------ #

    @property
    def latest(self) -> Metric | None:
        self._trim()
        return self._buf[-1] if self._buf else None

    @property
    def avg(self) -> float | None:
        self._trim()
        if not self._buf:
            return None
        return sum(m.value for m in self._buf) / len(self._buf)

    @property
    def p95(self) -> float | None:
        self._trim()
        if not self._buf:
            return None
        values = sorted(m.value for m in self._buf)
        idx = max(0, int(len(values) * 0.95) - 1)
        return values[idx]

    def __repr__(self) -> str:
        return f"MetricSeries({self.name!r}, samples={len(self._buf)})"


class MetricsCollector:
    """
    Multi-series metrics collector.

    Keeps one MetricSeries per unique metric name. New series are created
    lazily on first record call.
    """

    def __init__(self, default_retention: float = 300.0) -> None:
        self._series: dict[str, MetricSeries] = {}
        self._default_retention = default_retention

    def record(self, name: str, value: float, **labels: str) -> None:
        if name not in self._series:
            self._series[name] = MetricSeries(name, self._default_retention)
        self._series[name].record(value, **labels)

    def series(self, name: str) -> MetricSeries | None:
        return self._series.get(name)

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def with_retention(cls, seconds: float) -> "MetricsCollector":
        return cls(default_retention=seconds)

    @classmethod
    def for_pipeline(cls, pipeline_id: str) -> "MetricsCollector":
        """Pre-wired collector with standard pipeline metric names."""
        collector = cls()
        for name in ("job_duration", "queue_depth", "artifact_hit_rate", "error_rate"):
            collector._series[f"{pipeline_id}.{name}"] = MetricSeries(
                f"{pipeline_id}.{name}", collector._default_retention
            )
        return collector

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def merge_labels(*label_dicts: dict[str, str]) -> dict[str, str]:
        merged: dict[str, str] = {}
        for d in label_dicts:
            merged.update(d)
        return merged

    # ------------------------------------------------------------------ #
    # Iterable over all series                                             #
    # ------------------------------------------------------------------ #

    def __iter__(self) -> Iterator[MetricSeries]:
        return iter(self._series.values())

    def __len__(self) -> int:
        return len(self._series)

    def __contains__(self, name: str) -> bool:
        return name in self._series

    # ------------------------------------------------------------------ #
    # MetricBackend protocol implementation                               #
    # ------------------------------------------------------------------ #

    def push(self, metric: Metric) -> None:
        self.record(metric.name, metric.value, **metric.labels)

    def query(self, name: str, window: float) -> list[Metric]:
        s = self.series(name)
        if s is None:
            return []
        cutoff = time.time() - window
        return [m for m in s if m.timestamp >= cutoff]

    def __repr__(self) -> str:
        return f"MetricsCollector(series={list(self._series)})"
