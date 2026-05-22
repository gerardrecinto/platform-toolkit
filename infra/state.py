"""Immutable state store with snapshot history and descriptor-based access."""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field
from typing import Any, Generator, Generic, TypeVar

T = TypeVar("T")


class _TypedField(Generic[T]):
    """
    Descriptor that enforces type on assignment and tracks dirty state.
    Demonstrates the descriptor protocol (__set_name__, __get__, __set__).
    """

    def __init__(self, expected_type: type[T]) -> None:
        self._type = expected_type
        self._attr: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr = f"_{name}"

    def __get__(self, obj: Any, objtype: type | None = None) -> T | None:
        if obj is None:
            return self  # type: ignore[return-value]
        return getattr(obj, self._attr, None)

    def __set__(self, obj: Any, value: T) -> None:
        if not isinstance(value, self._type):
            raise TypeError(f"Expected {self._type.__name__}, got {type(value).__name__}")
        setattr(obj, self._attr, value)


@dataclass(slots=True, frozen=True)
class Snapshot:
    """A point-in-time capture of state data."""

    label: str
    data: dict[str, Any]
    taken_at: float = field(default_factory=time.time)
    parent_label: str | None = None

    def diff(self, other: "Snapshot") -> dict[str, tuple[Any, Any]]:
        """Return {key: (self_value, other_value)} for keys that changed."""
        all_keys = set(self.data) | set(other.data)
        return {
            k: (self.data.get(k), other.data.get(k))
            for k in all_keys
            if self.data.get(k) != other.data.get(k)
        }

    @property
    def age(self) -> float:
        return time.time() - self.taken_at

    def __repr__(self) -> str:
        return f"Snapshot({self.label!r}, keys={list(self.data)}, age={self.age:.1f}s)"


class StateStore:
    """
    Manages mutable state with full snapshot history.

    Key design decisions:
    - All external reads return deep copies so callers can't mutate internal state.
    - Snapshots are taken lazily — only computed when explicitly requested.
    - History is iterable oldest-to-newest via the iterator protocol.
    """

    version: _TypedField[int] = _TypedField(int)

    def __init__(self, name: str) -> None:
        self.name = name
        self.version = 0
        self._state: dict[str, Any] = {}
        self._snapshots: list[Snapshot] = []

    # ------------------------------------------------------------------ #
    # State mutation                                                       #
    # ------------------------------------------------------------------ #

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value
        self.version += 1

    def update(self, data: dict[str, Any]) -> None:
        self._state.update(data)
        self.version += 1

    def delete(self, key: str) -> bool:
        existed = key in self._state
        self._state.pop(key, None)
        if existed:
            self.version += 1
        return existed

    # ------------------------------------------------------------------ #
    # Getters — always return deep copies for isolation                    #
    # ------------------------------------------------------------------ #

    def get(self, key: str, default: Any = None) -> Any:
        val = self._state.get(key, default)
        return copy.deepcopy(val)

    def all(self) -> dict[str, Any]:
        return copy.deepcopy(self._state)

    # ------------------------------------------------------------------ #
    # Snapshots                                                            #
    # ------------------------------------------------------------------ #

    def take_snapshot(self, label: str) -> Snapshot:
        parent = self._snapshots[-1].label if self._snapshots else None
        snap = Snapshot(
            label=label,
            data=copy.deepcopy(self._state),
            parent_label=parent,
        )
        self._snapshots.append(snap)
        return snap

    def restore(self, label: str) -> bool:
        """Roll back to a named snapshot — returns False if not found."""
        for snap in reversed(self._snapshots):
            if snap.label == label:
                self._state = copy.deepcopy(snap.data)
                self.version += 1
                return True
        return False

    # ------------------------------------------------------------------ #
    # Generators                                                           #
    # ------------------------------------------------------------------ #

    def history(self) -> Generator[Snapshot, None, None]:
        """Yield snapshots oldest-to-newest."""
        yield from self._snapshots

    def history_reversed(self) -> Generator[Snapshot, None, None]:
        """Yield snapshots newest-to-oldest."""
        yield from reversed(self._snapshots)

    def changes_since(self, label: str) -> Generator[tuple[str, Any], None, None]:
        """
        Yield (key, new_value) for every key that changed since the named snapshot.
        """
        target: Snapshot | None = None
        for snap in self._snapshots:
            if snap.label == label:
                target = snap
                break
        if target is None:
            return
        for key, current_val in self._state.items():
            if target.data.get(key) != current_val:
                yield key, copy.deepcopy(current_val)

    # ------------------------------------------------------------------ #
    # Iterator protocol — iterate over current state keys                  #
    # ------------------------------------------------------------------ #

    def __iter__(self):
        return iter(self._state)

    def __contains__(self, key: str) -> bool:
        return key in self._state

    def __len__(self) -> int:
        return len(self._state)

    # ------------------------------------------------------------------ #
    # Copy semantics                                                       #
    # ------------------------------------------------------------------ #

    def fork(self, name: str | None = None) -> "StateStore":
        """
        Shallow fork — shares snapshot list reference (snapshots are frozen)
        but gives an independent mutable _state dict.
        """
        new = StateStore(name or f"{self.name}_fork")
        new._state = dict(self._state)
        new._snapshots = list(self._snapshots)
        new.version = self.version
        return new

    def clone(self, name: str | None = None) -> "StateStore":
        """Full deep copy — completely independent history and state."""
        new = StateStore(name or f"{self.name}_clone")
        new._state = copy.deepcopy(self._state)
        new._snapshots = copy.deepcopy(self._snapshots)
        new.version = self.version
        return new

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def merge(*stores: "StateStore", name: str = "merged") -> "StateStore":
        """
        Merge multiple state stores — later stores win on key conflicts.
        Returns a brand new StateStore; does not modify any input.
        """
        merged = StateStore(name)
        for store in stores:
            merged.update(copy.deepcopy(store._state))
        return merged

    @classmethod
    def from_snapshot(cls, snap: Snapshot) -> "StateStore":
        store = cls(snap.label)
        store._state = copy.deepcopy(snap.data)
        store._snapshots.append(snap)
        return store

    def __repr__(self) -> str:
        return (
            f"StateStore({self.name!r}, "
            f"keys={list(self._state)}, "
            f"snapshots={len(self._snapshots)}, "
            f"version={self.version})"
        )
