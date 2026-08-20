"""Rule-based alerting engine with abstract base class and generator evaluation."""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Generator, Sequence

from .collector import MetricSeries


class AlertSeverity(Enum):
    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()
    PAGE = auto()


@dataclass(slots=True, frozen=True)
class Alert:
    rule_name: str
    severity: AlertSeverity
    message: str
    metric_name: str
    observed_value: float
    threshold: float
    fired_at: float = field(default_factory=time.time)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.fired_at

    def __str__(self) -> str:
        return (
            f"[{self.severity.name}] {self.rule_name}: "
            f"{self.metric_name}={self.observed_value:.2f} "
            f"(threshold={self.threshold:.2f})"
        )


class AlertRule(ABC):
    """
    Abstract base for alert rules.
    Subclasses implement evaluate() and get wired into AlertEngine.
    """

    def __init__(self, name: str, severity: AlertSeverity) -> None:
        self.name = name
        self.severity = severity
        self._last_fired: float = 0.0
        self._cooldown: float = 60.0

    @abstractmethod
    def evaluate(self, series: MetricSeries) -> Alert | None:
        """Return an Alert if the rule fires, else None."""
        ...

    def _in_cooldown(self) -> bool:
        return (time.time() - self._last_fired) < self._cooldown

    def _mark_fired(self) -> None:
        self._last_fired = time.time()

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def threshold(
        cls,
        name: str,
        metric: str,
        above: float,
        severity: AlertSeverity = AlertSeverity.WARNING,
    ) -> "ThresholdRule":
        return ThresholdRule(name=name, metric_name=metric, threshold=above, severity=severity)

    @classmethod
    def rate_of_change(
        cls,
        name: str,
        metric: str,
        max_delta: float,
        severity: AlertSeverity = AlertSeverity.CRITICAL,
    ) -> "RateOfChangeRule":
        return RateOfChangeRule(name=name, metric_name=metric, max_delta=max_delta, severity=severity)

    @classmethod
    def absence(
        cls,
        name: str,
        metric: str,
        after_seconds: float,
        severity: AlertSeverity = AlertSeverity.CRITICAL,
    ) -> "AbsenceRule":
        return AbsenceRule(name=name, metric_name=metric, after_seconds=after_seconds, severity=severity)

    def with_cooldown(self, seconds: float) -> "AlertRule":
        """Fluent setter for cooldown duration — returns self."""
        self._cooldown = seconds
        return self

    # ------------------------------------------------------------------ #
    # Copy semantics                                                       #
    # ------------------------------------------------------------------ #

    def clone(self) -> "AlertRule":
        """Shallow copy — safe because rule config is immutable after init."""
        return copy.copy(self)


class ThresholdRule(AlertRule):
    """Fires when the latest metric value exceeds a static threshold."""

    def __init__(
        self,
        name: str,
        metric_name: str,
        threshold: float,
        severity: AlertSeverity,
    ) -> None:
        super().__init__(name, severity)
        self.metric_name = metric_name
        self.threshold = threshold

    def evaluate(self, series: MetricSeries) -> Alert | None:
        if self._in_cooldown():
            return None
        latest = series.latest
        if latest is None:
            return None
        if latest.value > self.threshold:
            self._mark_fired()
            return Alert(
                rule_name=self.name,
                severity=self.severity,
                message=f"{self.metric_name} exceeded threshold",
                metric_name=self.metric_name,
                observed_value=latest.value,
                threshold=self.threshold,
            )
        return None


class RateOfChangeRule(AlertRule):
    """Fires when the metric changes faster than max_delta per sample."""

    def __init__(
        self,
        name: str,
        metric_name: str,
        max_delta: float,
        severity: AlertSeverity,
    ) -> None:
        super().__init__(name, severity)
        self.metric_name = metric_name
        self.max_delta = max_delta

    def evaluate(self, series: MetricSeries) -> Alert | None:
        if self._in_cooldown():
            return None
        samples = list(series)
        if len(samples) < 2:
            return None
        delta = abs(samples[-1].value - samples[-2].value)
        if delta > self.max_delta:
            self._mark_fired()
            return Alert(
                rule_name=self.name,
                severity=self.severity,
                message=f"{self.metric_name} changed by {delta:.2f} in one sample",
                metric_name=self.metric_name,
                observed_value=delta,
                threshold=self.max_delta,
            )
        return None


class AbsenceRule(AlertRule):
    """
    Dead man's switch — fires when a metric hasn't reported in over
    after_seconds. Useful for detecting a job or agent that stopped
    reporting entirely, which threshold/rate rules can't catch since
    they only ever look at samples that exist.
    """

    def __init__(
        self,
        name: str,
        metric_name: str,
        after_seconds: float,
        severity: AlertSeverity,
    ) -> None:
        super().__init__(name, severity)
        self.metric_name = metric_name
        self.after_seconds = after_seconds

    def evaluate(self, series: MetricSeries) -> Alert | None:
        if self._in_cooldown():
            return None
        latest = series.latest
        now = time.time()
        if latest is None:
            age = self.after_seconds
        else:
            age = now - latest.timestamp
            if age <= self.after_seconds:
                return None
        self._mark_fired()
        return Alert(
            rule_name=self.name,
            severity=self.severity,
            message=f"{self.metric_name} has not reported in {age:.0f}s",
            metric_name=self.metric_name,
            observed_value=age,
            threshold=self.after_seconds,
        )


class AlertEngine:
    """
    Wires rules to metric series and evaluates them lazily via generators.
    """

    def __init__(self) -> None:
        self._rules: list[tuple[AlertRule, str]] = []
        self._fired: list[Alert] = []

    def register(self, rule: AlertRule, metric_name: str) -> "AlertEngine":
        self._rules.append((rule, metric_name))
        return self

    def evaluate(
        self, series_map: dict[str, MetricSeries]
    ) -> Generator[Alert, None, None]:
        """Lazily evaluate all rules and yield any that fire."""
        for rule, metric_name in self._rules:
            series = series_map.get(metric_name)
            if series is None:
                continue
            alert = rule.evaluate(series)
            if alert is not None:
                self._fired.append(alert)
                yield alert

    def critical_alerts(
        self, series_map: dict[str, MetricSeries]
    ) -> Generator[Alert, None, None]:
        """Convenience generator — only PAGE and CRITICAL severity."""
        for alert in self.evaluate(series_map):
            if alert.severity in (AlertSeverity.CRITICAL, AlertSeverity.PAGE):
                yield alert

    @property
    def history(self) -> list[Alert]:
        return list(self._fired)

    @staticmethod
    def deduplicate(alerts: Sequence[Alert]) -> list[Alert]:
        """Remove duplicate rule firings within the same 60s window."""
        seen: dict[str, float] = {}
        result: list[Alert] = []
        for a in alerts:
            last = seen.get(a.rule_name, 0.0)
            if (a.fired_at - last) >= 60.0:
                result.append(a)
                seen[a.rule_name] = a.fired_at
        return result

    def __len__(self) -> int:
        return len(self._rules)

    def __repr__(self) -> str:
        return f"AlertEngine(rules={len(self._rules)}, fired={len(self._fired)})"
