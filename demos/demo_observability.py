#!/usr/bin/env python3
"""
Demonstrates the observability module: metrics collection, log aggregation,
and rule-based alerting.
"""

import time

from observability import MetricsCollector, MetricSeries, LogAggregator, AlertEngine, AlertRule, AlertSeverity


SAMPLE_LOGS = [
    "2024-03-01T12:00:00.000Z [INFO] [job/build-main] Starting build step",
    "2024-03-01T12:00:01.000Z [INFO] [job/build-main] Pulling Docker layer sha256:abc123",
    "2024-03-01T12:00:05.000Z [WARN] [job/lint-pr] Line too long: api/routes.py:143",
    "2024-03-01T12:00:06.000Z [ERROR] [job/unit-tests] AssertionError: expected 200, got 404",
    "2024-03-01T12:00:07.000Z [INFO] [job/unit-tests] Retrying (1/3)...",
    "2024-03-01T12:00:10.000Z [ERROR] [job/unit-tests] Max retries exceeded",
    "2024-03-01T12:00:11.000Z [INFO] [job/build-main] Build complete: 8.2s",
    "2024-03-01T12:00:12.000Z [FATAL] [job/deploy-dev] kubectl: connection refused",
    "not a valid log line",
    "2024-03-01T12:00:13.000Z [INFO] [job/smoke] Skipping — deploy failed",
]


def demo_metrics():
    print("=" * 60)
    print("MetricsCollector — Protocol-backed, sliding-window series")
    print("=" * 60)

    collector = MetricsCollector.with_retention(300.0)

    # Simulate job duration metrics
    durations = [8.2, 12.1, 7.9, 15.3, 9.4, 11.2, 8.8, 10.1, 13.0, 9.6]
    for d in durations:
        collector.record("job_duration_seconds", d, job="build-main", stage="docker")

    # Simulate queue depth
    for depth in range(10, 0, -1):
        collector.record("queue_depth", float(depth))

    print(f"\nCollector: {collector}")

    series = collector.series("job_duration_seconds")
    assert series is not None

    print(f"\nJob duration series:")
    print(f"  samples : {len(series)}")
    print(f"  avg     : {series.avg:.2f}s")
    print(f"  p95     : {series.p95:.2f}s")
    print(f"  latest  : {series.latest.value:.2f}s")

    # Iterating a MetricSeries
    print(f"\nLast 3 samples:")
    samples = list(series)
    for m in samples[-3:]:
        print(f"  {m.value:.1f}s  labels={m.labels}")

    # Protocol check
    from observability.collector import MetricBackend
    print(f"\nMetricsCollector satisfies MetricBackend protocol: {isinstance(collector, MetricBackend)}")


def demo_log_aggregator():
    print("\n" + "=" * 60)
    print("LogAggregator — streaming parsing with generator pipelines")
    print("=" * 60)

    agg = LogAggregator()

    # Generator pipeline: parse → filter errors
    entries = list(agg.parse_stream(iter(SAMPLE_LOGS)))
    print(f"\nParsed {len(entries)} valid entries from {len(SAMPLE_LOGS)} raw lines")

    errors = list(agg.errors_only(iter(entries)))
    print(f"Errors found: {len(errors)}")
    for e in errors:
        print(f"  [{e.level}] {e.job_id}: {e.message}")

    print("\nFirst failure across all logs:")
    first = agg.first_failure(iter(entries))
    if first:
        print(f"  {first.job_id}: {first.message}")

    print("\nLevel counts:")
    counts = LogAggregator.level_counts(iter(entries))
    for level, count in sorted(counts.items()):
        print(f"  {level:8s}: {count}")

    print("\nPer-job summary:")
    for summary in agg.summarise(iter(entries)):
        print(f"  {summary['job_id']:20s}  total={summary['total']}  errors={summary['errors']}")


def demo_alerts():
    print("\n" + "=" * 60)
    print("AlertEngine — ABC rules with generator evaluation")
    print("=" * 60)

    collector = MetricsCollector()

    # Build up a series that will breach threshold
    for v in [0.01, 0.02, 0.03, 0.08, 0.15, 0.22, 0.41]:
        collector.record("error_rate", v)

    for v in [2, 5, 10, 18, 12, 8, 3]:
        collector.record("queue_depth", float(v))

    engine = AlertEngine()
    engine.register(
        AlertRule.threshold("high-error-rate", "error_rate", above=0.1, severity=AlertSeverity.CRITICAL).with_cooldown(0),
        "error_rate",
    )
    engine.register(
        AlertRule.threshold("queue-saturation", "queue_depth", above=15.0, severity=AlertSeverity.WARNING).with_cooldown(0),
        "queue_depth",
    )
    engine.register(
        AlertRule.rate_of_change("error-spike", "error_rate", max_delta=0.1, severity=AlertSeverity.PAGE).with_cooldown(0),
        "error_rate",
    )

    series_map = {name: s for name, s in [(s.name, s) for s in collector]}
    print(f"\nEvaluating {len(engine)} rules...")

    for alert in engine.evaluate(series_map):
        print(f"  {alert}")

    print(f"\nTotal fired: {len(engine.history)}")


if __name__ == "__main__":
    demo_metrics()
    demo_log_aggregator()
    demo_alerts()
