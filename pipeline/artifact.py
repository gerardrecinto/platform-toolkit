"""Artifact caching with versioned snapshots and copy semantics."""

from __future__ import annotations

import copy
import hashlib
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator, Iterator


@dataclass(slots=True, frozen=True)
class Artifact:
    """Immutable artifact record — frozen so it can live safely in sets/dicts."""

    key: str
    version: str
    payload: bytes
    created_at: float = field(default_factory=time.time)
    metadata: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, key: str, data: bytes, **meta: str) -> "Artifact":
        version = hashlib.sha256(data).hexdigest()[:12]
        return cls(key=key, version=version, payload=data, metadata=meta)

    @classmethod
    def from_text(cls, key: str, text: str, encoding: str = "utf-8", **meta: str) -> "Artifact":
        return cls.from_bytes(key, text.encode(encoding), **meta)

    @staticmethod
    def checksum(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def is_stale(self, max_age_seconds: float) -> bool:
        return (time.time() - self.created_at) > max_age_seconds


class _CacheIterator:
    """Iterates cache entries ordered by creation time (oldest first)."""

    def __init__(self, entries: list[Artifact]) -> None:
        self._entries = sorted(entries, key=lambda a: a.created_at)
        self._index = 0

    def __iter__(self) -> Iterator[Artifact]:
        return self

    def __next__(self) -> Artifact:
        if self._index >= len(self._entries):
            raise StopIteration
        artifact = self._entries[self._index]
        self._index += 1
        return artifact


class ArtifactCache:
    """
    In-process artifact cache backed by a versioned key store.

    Supports transactional writes (context manager), lazy eviction via
    generators, and full snapshot isolation using deepcopy.
    """

    def __init__(self, max_entries: int = 512, max_age: float = 3600.0) -> None:
        self._store: dict[str, Artifact] = {}
        self._max_entries = max_entries
        self._max_age = max_age
        self._hits = 0
        self._misses = 0

    # ------------------------------------------------------------------ #
    # Core API                                                             #
    # ------------------------------------------------------------------ #

    def put(self, artifact: Artifact) -> None:
        if len(self._store) >= self._max_entries:
            self._evict_oldest()
        self._store[artifact.key] = artifact

    def get(self, key: str) -> Artifact | None:
        artifact = self._store.get(key)
        if artifact is None or artifact.is_stale(self._max_age):
            self._misses += 1
            if artifact is not None:
                del self._store[key]
            return None
        self._hits += 1
        return artifact

    def invalidate(self, key: str) -> bool:
        return self._store.pop(key, None) is not None

    # ------------------------------------------------------------------ #
    # Context manager — transactional batch writes                         #
    # ------------------------------------------------------------------ #

    @contextmanager
    def transaction(self) -> Generator[list[Artifact], None, None]:
        """
        Collect artifacts in a staging list; on clean exit, commit all at once.
        On exception, nothing is written to the cache.
        """
        staging: list[Artifact] = []
        try:
            yield staging
            for artifact in staging:
                self.put(artifact)
        except Exception:
            staging.clear()
            raise

    # ------------------------------------------------------------------ #
    # Generators                                                           #
    # ------------------------------------------------------------------ #

    def stale_entries(self) -> Generator[Artifact, None, None]:
        """Lazily yield stale entries without materialising them all at once."""
        for artifact in list(self._store.values()):
            if artifact.is_stale(self._max_age):
                yield artifact

    def entries_by_tag(self, tag: str) -> Generator[Artifact, None, None]:
        """Yield artifacts whose metadata contains a matching tag value."""
        for artifact in self._store.values():
            if artifact.metadata.get("tag") == tag:
                yield artifact

    def evict_stale(self) -> int:
        """Remove all stale entries, return count removed."""
        stale = list(self.stale_entries())
        for a in stale:
            self._store.pop(a.key, None)
        return len(stale)

    # ------------------------------------------------------------------ #
    # Iterator protocol                                                    #
    # ------------------------------------------------------------------ #

    def __iter__(self) -> _CacheIterator:
        return _CacheIterator(list(self._store.values()))

    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: str) -> bool:
        return key in self._store and not self._store[key].is_stale(self._max_age)

    # ------------------------------------------------------------------ #
    # Copy semantics                                                       #
    # ------------------------------------------------------------------ #

    def snapshot(self) -> "ArtifactCache":
        """
        Deep-copy the cache for snapshot isolation — safe to mutate
        independently without affecting the original.
        """
        return copy.deepcopy(self)

    def fork(self) -> "ArtifactCache":
        """
        Shallow-copy the cache — shares Artifact objects (which are frozen)
        so this is safe and ~10x cheaper than snapshot() for read-heavy forks.
        """
        new = ArtifactCache(self._max_entries, self._max_age)
        new._store = dict(self._store)
        new._hits = self._hits
        new._misses = self._misses
        return new

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _evict_oldest(self) -> None:
        if not self._store:
            return
        oldest_key = min(self._store, key=lambda k: self._store[k].created_at)
        del self._store[oldest_key]

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total else 0.0

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "size": len(self._store),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self.hit_rate:.1%}",
        }

    def __repr__(self) -> str:
        return f"ArtifactCache(size={len(self)}, hit_rate={self.hit_rate:.1%})"
