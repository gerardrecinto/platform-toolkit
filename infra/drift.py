"""Infrastructure drift detection — compares desired vs actual state lazily."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Generator


class DriftType(Enum):
    ADDED = auto()
    REMOVED = auto()
    CHANGED = auto()
    UNCHANGED = auto()


@dataclass(slots=True, frozen=True)
class DriftResult:
    path: str
    drift_type: DriftType
    desired: Any
    actual: Any

    @property
    def is_drift(self) -> bool:
        return self.drift_type is not DriftType.UNCHANGED

    def __str__(self) -> str:
        match self.drift_type:
            case DriftType.ADDED:
                return f"+ {self.path}: {self.actual!r}"
            case DriftType.REMOVED:
                return f"- {self.path}: {self.desired!r}"
            case DriftType.CHANGED:
                return f"~ {self.path}: {self.desired!r} → {self.actual!r}"
            case DriftType.UNCHANGED:
                return f"  {self.path}: {self.actual!r}"


class DriftDetector:
    """
    Recursive drift detector using generators for lazy evaluation.

    Only walks the actual tree on demand — nothing is computed until
    you iterate the result.
    """

    def __init__(self, ignore_keys: set[str] | None = None) -> None:
        self._ignore = ignore_keys or set()

    # ------------------------------------------------------------------ #
    # Generator-based comparison                                           #
    # ------------------------------------------------------------------ #

    def compare(
        self,
        desired: dict[str, Any],
        actual: dict[str, Any],
        prefix: str = "",
    ) -> Generator[DriftResult, None, None]:
        """
        Recursively yield DriftResult for every key in the union of both dicts.
        Uses `yield from` for nested dicts.
        """
        all_keys = set(desired) | set(actual)
        for key in sorted(all_keys):
            if key in self._ignore:
                continue
            path = f"{prefix}.{key}" if prefix else key
            desired_val = desired.get(key)
            actual_val = actual.get(key)

            if key not in actual:
                yield DriftResult(path, DriftType.REMOVED, desired_val, None)
            elif key not in desired:
                yield DriftResult(path, DriftType.ADDED, None, actual_val)
            elif isinstance(desired_val, dict) and isinstance(actual_val, dict):
                # recursive — yield from flattens the inner generator into this one
                yield from self.compare(desired_val, actual_val, prefix=path)
            elif desired_val != actual_val:
                yield DriftResult(path, DriftType.CHANGED, desired_val, actual_val)
            else:
                yield DriftResult(path, DriftType.UNCHANGED, desired_val, actual_val)

    def drifted_only(
        self, desired: dict, actual: dict
    ) -> Generator[DriftResult, None, None]:
        """Convenience wrapper that filters out UNCHANGED results."""
        yield from (r for r in self.compare(desired, actual) if r.is_drift)

    def summary(self, desired: dict, actual: dict) -> dict[str, int]:
        """Materialise the generator and return counts per drift type."""
        counts: dict[str, int] = {dt.name: 0 for dt in DriftType}
        for result in self.compare(desired, actual):
            counts[result.drift_type.name] += 1
        return counts

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def for_kubernetes(cls) -> "DriftDetector":
        """Pre-configured to ignore volatile K8s metadata fields."""
        return cls(
            ignore_keys={
                "resourceVersion",
                "uid",
                "creationTimestamp",
                "generation",
                "managedFields",
            }
        )

    @classmethod
    def for_terraform(cls) -> "DriftDetector":
        """Pre-configured to ignore Terraform-managed computed fields."""
        return cls(ignore_keys={"id", "arn", "created_at", "updated_at"})

    # ------------------------------------------------------------------ #
    # Static utilities                                                     #
    # ------------------------------------------------------------------ #

    @staticmethod
    def flatten(nested: dict, prefix: str = "") -> Generator[tuple[str, Any], None, None]:
        """
        Generator that flattens a nested dict into (dotted.key, value) pairs.
        Used to pre-process configs before comparison.
        """
        for key, value in nested.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                yield from DriftDetector.flatten(value, prefix=path)
            else:
                yield path, value

    @staticmethod
    def patch(base: dict, results: list[DriftResult]) -> dict:
        """
        Apply a list of drift results back onto base to produce desired state.
        Returns a deep copy — never mutates the input.
        """
        patched = copy.deepcopy(base)
        for r in results:
            keys = r.path.split(".")
            target = patched
            for k in keys[:-1]:
                target = target.setdefault(k, {})
            match r.drift_type:
                case DriftType.REMOVED:
                    target.pop(keys[-1], None)
                case DriftType.ADDED | DriftType.CHANGED:
                    target[keys[-1]] = r.desired
        return patched
