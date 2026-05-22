"""Tests for pipeline.artifact"""

import time
import pytest

from pipeline.artifact import ArtifactCache, Artifact


def make_cache(**kwargs) -> ArtifactCache:
    return ArtifactCache(**kwargs)


def test_put_and_get():
    cache = make_cache()
    a = Artifact.from_text("key1", "hello")
    cache.put(a)
    result = cache.get("key1")
    assert result is not None
    assert result.key == "key1"


def test_miss_returns_none():
    cache = make_cache()
    assert cache.get("nonexistent") is None


def test_stale_entry_evicted_on_get():
    cache = make_cache(max_age=0.01)
    a = Artifact.from_text("key1", "hello")
    cache.put(a)
    time.sleep(0.02)
    assert cache.get("key1") is None


def test_transaction_commits_on_success():
    cache = make_cache()
    with cache.transaction() as staging:
        staging.append(Artifact.from_text("a", "content-a"))
        staging.append(Artifact.from_text("b", "content-b"))
    assert "a" in cache
    assert "b" in cache


def test_transaction_rolls_back_on_error():
    cache = make_cache()
    with pytest.raises(ValueError):
        with cache.transaction() as staging:
            staging.append(Artifact.from_text("x", "content"))
            raise ValueError("simulated failure")
    assert "x" not in cache


def test_fork_is_independent():
    cache = make_cache()
    cache.put(Artifact.from_text("shared", "data"))
    forked = cache.fork()
    forked.invalidate("shared")
    assert "shared" in cache
    assert "shared" not in forked


def test_snapshot_is_independent():
    cache = make_cache()
    cache.put(Artifact.from_text("key", "original"))
    snap = cache.snapshot()
    cache.invalidate("key")
    assert "key" not in cache
    assert "key" in snap


def test_iter_order():
    cache = make_cache()
    for key in ["c", "a", "b"]:
        time.sleep(0.001)
        cache.put(Artifact.from_text(key, key))
    keys = [a.key for a in cache]
    assert keys == sorted(keys, key=lambda k: cache._store[k].created_at)


def test_evict_stale():
    cache = make_cache(max_age=0.01)
    for k in ["x", "y", "z"]:
        cache.put(Artifact.from_text(k, k))
    time.sleep(0.02)
    evicted = cache.evict_stale()
    assert evicted == 3
    assert len(cache) == 0


def test_hit_rate():
    cache = make_cache()
    cache.put(Artifact.from_text("k", "v"))
    cache.get("k")
    cache.get("missing")
    assert cache.hit_rate == 0.5


def test_checksum():
    data = b"hello"
    cs = Artifact.checksum(data)
    assert len(cs) == 64  # SHA-256 hex
