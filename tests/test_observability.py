"""Tests for observability.collector, observability.aggregator, observability.alerts"""

import time

from observability.aggregator import LogAggregator, LogEntry
from observability.alerts import AlertEngine, AlertRule, AlertSeverity
from observability.collector import Metric, MetricsCollector, MetricSeries

# ---------------------------------------------------------------------- #
# collector
# ---------------------------------------------------------------------- #


def test_metric_gauge_and_counter():
    g = Metric.gauge("queue_depth", 5, region="us-west")
    assert g.name == "queue_depth"
    assert g.labels == {"region": "us-west"}
    c = Metric.counter("errors", 2, job="build")
    assert c.name == "errors_total"
    assert c.value == 2


def test_metric_with_label_is_immutable():
    m = Metric.gauge("cpu", 0.5)
    m2 = m.with_label("host", "node1")
    assert m.labels == {}
    assert m2.labels == {"host": "node1"}


def test_series_record_and_latest():
    series = MetricSeries("cpu")
    series.record(0.1)
    series.record(0.2)
    assert series.latest.value == 0.2
    assert len(series) == 2


def test_series_avg_and_p95():
    series = MetricSeries("cpu")
    for v in [1, 2, 3, 4, 5]:
        series.record(v)
    assert series.avg == 3.0
    assert series.p95 == 4


def test_series_empty_aggregates_are_none():
    series = MetricSeries("cpu")
    assert series.latest is None
    assert series.avg is None
    assert series.p95 is None


def test_series_trims_stale_entries():
    series = MetricSeries("cpu", retention=0.01)
    series.record(1.0)
    time.sleep(0.02)
    assert len(series) == 0


def test_collector_record_creates_series():
    c = MetricsCollector()
    c.record("cpu", 0.5)
    assert "cpu" in c
    assert len(c) == 1


def test_collector_push_and_query_protocol():
    c = MetricsCollector()
    c.push(Metric.gauge("cpu", 0.7))
    results = c.query("cpu", window=60)
    assert len(results) == 1
    assert results[0].value == 0.7


def test_collector_for_pipeline_prewires_series():
    c = MetricsCollector.for_pipeline("build-1")
    assert "build-1.job_duration" in c
    assert "build-1.error_rate" in c


def test_collector_merge_labels():
    merged = MetricsCollector.merge_labels({"a": "1"}, {"b": "2"}, {"a": "3"})
    assert merged == {"a": "3", "b": "2"}


# ---------------------------------------------------------------------- #
# aggregator
# ---------------------------------------------------------------------- #

LOG_LINES = [
    "2024-03-01T12:00:00.000Z [INFO] [job/build] Starting step",
    "2024-03-01T12:00:01.000Z [ERROR] [job/build] Compile failed",
    "2024-03-01T12:00:02.000Z [WARN] [job/test] Flaky test retried",
    "not a log line at all",
]


def test_log_entry_parse():
    entry = LogEntry.parse(LOG_LINES[0])
    assert entry is not None
    assert entry.level == "INFO"
    assert entry.job_id == "job/build"


def test_log_entry_parse_skips_unparseable():
    assert LogEntry.parse(LOG_LINES[3]) is None


def test_parse_stream_skips_bad_lines():
    agg = LogAggregator()
    entries = list(agg.parse_stream(LOG_LINES))
    assert len(entries) == 3


def test_errors_only():
    agg = LogAggregator()
    entries = agg.parse_stream(LOG_LINES)
    errors = list(agg.errors_only(entries))
    assert len(errors) == 1
    assert errors[0].level == "ERROR"


def test_by_job():
    agg = LogAggregator()
    entries = agg.parse_stream(LOG_LINES)
    build_entries = list(agg.by_job(entries, "job/build"))
    assert len(build_entries) == 2


def test_summarise():
    agg = LogAggregator()
    entries = agg.parse_stream(LOG_LINES)
    summaries = {s["job_id"]: s for s in agg.summarise(entries)}
    assert summaries["job/build"]["total"] == 2
    assert summaries["job/build"]["errors"] == 1
    assert summaries["job/test"]["errors"] == 0


def test_first_failure():
    agg = LogAggregator()
    entries = agg.parse_stream(LOG_LINES)
    first = agg.first_failure(entries)
    assert first is not None
    assert first.level == "ERROR"


def test_level_counts():
    agg = LogAggregator()
    entries = agg.parse_stream(LOG_LINES)
    counts = LogAggregator.level_counts(entries)
    assert counts == {"INFO": 1, "ERROR": 1, "WARN": 1}


def test_chain_sources():
    agg = LogAggregator()
    entries = list(agg.chain_sources(LOG_LINES[:2], LOG_LINES[2:]))
    assert len(entries) == 3


# ---------------------------------------------------------------------- #
# alerts
# ---------------------------------------------------------------------- #


def test_threshold_rule_fires_above_threshold():
    series = MetricSeries("error_rate")
    series.record(0.5)
    rule = AlertRule.threshold("high-errors", "error_rate", above=0.1)
    alert = rule.evaluate(series)
    assert alert is not None
    assert alert.severity == AlertSeverity.WARNING


def test_threshold_rule_respects_cooldown():
    series = MetricSeries("error_rate")
    series.record(0.5)
    rule = AlertRule.threshold("high-errors", "error_rate", above=0.1).with_cooldown(60)
    assert rule.evaluate(series) is not None
    series.record(0.6)
    assert rule.evaluate(series) is None  # still in cooldown


def test_threshold_rule_no_data_no_alert():
    series = MetricSeries("error_rate")
    rule = AlertRule.threshold("high-errors", "error_rate", above=0.1)
    assert rule.evaluate(series) is None


def test_rate_of_change_rule():
    series = MetricSeries("cpu")
    series.record(0.1)
    series.record(0.9)
    rule = AlertRule.rate_of_change("cpu-spike", "cpu", max_delta=0.5)
    alert = rule.evaluate(series)
    assert alert is not None
    assert alert.observed_value == 0.8


def test_absence_rule_fires_when_stale():
    series = MetricSeries("heartbeat")
    series.record(1.0)
    series._buf[-1] = Metric(name="heartbeat", value=1.0, timestamp=time.time() - 120)
    rule = AlertRule.absence("dead-man", "heartbeat", after_seconds=60)
    alert = rule.evaluate(series)
    assert alert is not None
    assert alert.severity == AlertSeverity.CRITICAL


def test_absence_rule_fires_when_no_data_at_all():
    series = MetricSeries("heartbeat")
    rule = AlertRule.absence("dead-man", "heartbeat", after_seconds=60)
    alert = rule.evaluate(series)
    assert alert is not None


def test_absence_rule_silent_when_recent():
    series = MetricSeries("heartbeat")
    series.record(1.0)
    rule = AlertRule.absence("dead-man", "heartbeat", after_seconds=60)
    assert rule.evaluate(series) is None


def test_alert_engine_evaluate_and_history():
    collector = MetricsCollector()
    collector.record("error_rate", 0.5)
    engine = AlertEngine()
    engine.register(AlertRule.threshold("high-errors", "error_rate", above=0.1), "error_rate")
    fired = list(engine.evaluate({"error_rate": collector.series("error_rate")}))
    assert len(fired) == 1
    assert len(engine.history) == 1


def test_alert_engine_critical_alerts_filters_severity():
    collector = MetricsCollector()
    collector.record("error_rate", 0.5)
    engine = AlertEngine()
    engine.register(
        AlertRule.threshold("high-errors", "error_rate", above=0.1, severity=AlertSeverity.INFO),
        "error_rate",
    )
    fired = list(engine.critical_alerts({"error_rate": collector.series("error_rate")}))
    assert fired == []


def test_alert_engine_deduplicate():
    from observability.alerts import Alert

    a1 = Alert("rule", AlertSeverity.WARNING, "msg", "m", 1.0, 0.5, fired_at=100.0)
    a2 = Alert("rule", AlertSeverity.WARNING, "msg", "m", 1.0, 0.5, fired_at=110.0)
    a3 = Alert("rule", AlertSeverity.WARNING, "msg", "m", 1.0, 0.5, fired_at=200.0)
    result = AlertEngine.deduplicate([a1, a2, a3])
    assert len(result) == 2
