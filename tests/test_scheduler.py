"""Tests for pipeline.scheduler"""

from pipeline.scheduler import JobScheduler, Priority


def test_submit_and_len():
    s = JobScheduler()
    s.submit("a", "echo a")
    s.submit("b", "echo b")
    assert len(s) == 2


def test_runnable_respects_max_concurrent():
    s = JobScheduler(max_concurrent=2)
    for i in range(5):
        s.submit(f"job{i}", "echo hi")
    runnable = list(s.runnable())
    assert len(runnable) == 2
    # the rest stay queued, ready to be picked up once slots free
    assert len(s) == 3


def test_runnable_yields_more_once_slots_free():
    s = JobScheduler(max_concurrent=2)
    for i in range(3):
        s.submit(f"job{i}", "echo hi")
    first_batch = list(s.runnable())
    assert len(first_batch) == 2
    s.mark_done(first_batch[0].job_id)
    second_batch = list(s.runnable())
    assert len(second_batch) == 1


def test_runnable_respects_delay():
    s = JobScheduler(max_concurrent=8)
    s.submit("later", "echo later", delay=100.0)
    assert list(s.runnable()) == []
    assert len(s) == 1


def test_runnable_priority_order():
    s = JobScheduler(max_concurrent=8)
    s.submit("low", "echo", priority=Priority.LOW)
    s.submit("critical", "echo", priority=Priority.CRITICAL)
    s.submit("normal", "echo", priority=Priority.NORMAL)
    order = [j.job_id for j in s.runnable()]
    assert order == ["critical", "normal", "low"]


def test_cancel_removes_queued_job():
    s = JobScheduler()
    s.submit("a", "echo a")
    s.submit("b", "echo b")
    assert s.cancel("a") is True
    assert len(s) == 1
    assert s.cancel("missing") is False


def test_by_tag():
    s = JobScheduler()
    s.submit("a", "echo a", tags=frozenset({"lint"}))
    s.submit("b", "echo b", tags=frozenset({"test"}))
    tagged = list(s.by_tag("lint"))
    assert [j.job_id for j in tagged] == ["a"]


def test_drain_empties_queue():
    s = JobScheduler()
    s.submit("a", "echo a")
    s.submit("b", "echo b")
    drained = list(s.drain())
    assert {j.job_id for j in drained} == {"a", "b"}
    assert len(s) == 0


def test_from_pipeline():
    jobs = [
        {"id": "build", "command": "make build", "priority": "high", "tags": ["ci"]},
        {"id": "test", "command": "make test"},
    ]
    s = JobScheduler.from_pipeline(jobs)
    assert len(s) == 2
    build = next(j for j in s if j.job_id == "build")
    assert build.priority == Priority.HIGH
    assert "ci" in build.tags


def test_estimate_wait():
    s = JobScheduler()
    s.submit("a", "echo a", priority=Priority.NORMAL)
    s.submit("b", "echo b", priority=Priority.NORMAL)
    wait = JobScheduler.estimate_wait(s._heap, Priority.NORMAL)
    assert wait == 60.0


def test_group_by_priority():
    s = JobScheduler()
    s.submit("a", "echo a", priority=Priority.HIGH)
    s.submit("b", "echo b", priority=Priority.HIGH)
    s.submit("c", "echo c", priority=Priority.LOW)
    groups = JobScheduler.group_by_priority(list(s))
    assert set(groups["HIGH"]) == {"a", "b"}
    assert groups["LOW"] == ["c"]


def test_iter_is_non_destructive():
    s = JobScheduler()
    s.submit("a", "echo a")
    list(s)
    assert len(s) == 1


def test_mark_done_frees_slot_and_records_completion():
    s = JobScheduler(max_concurrent=1)
    s.submit("a", "echo a")
    next(s.runnable())
    assert "a" in s._running
    s.mark_done("a")
    assert "a" not in s._running
    assert "a" in s._completed
