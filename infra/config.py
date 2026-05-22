"""Config loading, layered merging, and deep/shallow copy semantics."""

from __future__ import annotations

import copy
import json
from collections.abc import MutableMapping
from typing import Any, Generator, Iterator


class ConfigLayer:
    """
    A single layer in a layered config stack (e.g., defaults → env → file → override).

    Implements MutableMapping so it works anywhere a dict is expected.
    Uses __slots__ to keep per-layer overhead minimal when stacking many layers.
    """

    __slots__ = ("name", "_data", "priority")

    def __init__(self, name: str, data: dict[str, Any], priority: int = 0) -> None:
        self.name = name
        self._data: dict[str, Any] = dict(data)
        self.priority = priority

    # ------------------------------------------------------------------ #
    # MutableMapping protocol                                              #
    # ------------------------------------------------------------------ #

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    # ------------------------------------------------------------------ #
    # Copy semantics                                                       #
    # ------------------------------------------------------------------ #

    def shallow_clone(self) -> "ConfigLayer":
        """Shallow copy — nested dicts/lists are shared, not duplicated."""
        return copy.copy(self)

    def deep_clone(self) -> "ConfigLayer":
        """Deep copy — fully independent, safe to mutate nested structures."""
        return copy.deepcopy(self)

    def __copy__(self) -> "ConfigLayer":
        new = ConfigLayer.__new__(ConfigLayer)
        new.name = self.name
        new._data = dict(self._data)
        new.priority = self.priority
        return new

    def __deepcopy__(self, memo: dict) -> "ConfigLayer":
        new = ConfigLayer.__new__(ConfigLayer)
        new.name = self.name
        new._data = copy.deepcopy(self._data, memo)
        new.priority = self.priority
        return new

    @classmethod
    def from_json(cls, name: str, json_str: str, priority: int = 0) -> "ConfigLayer":
        return cls(name=name, data=json.loads(json_str), priority=priority)

    @staticmethod
    def _is_mergeable(value: Any) -> bool:
        return isinstance(value, dict)

    def __repr__(self) -> str:
        return f"ConfigLayer({self.name!r}, keys={list(self._data)}, priority={self.priority})"


class Config(MutableMapping):
    """
    Layered config that resolves keys from highest-priority layer downward.

    Merges nested dicts across layers (deep merge) rather than replacing them.
    Supports diff generation between snapshots.
    """

    def __init__(self) -> None:
        self._layers: list[ConfigLayer] = []

    # ------------------------------------------------------------------ #
    # Layer management                                                     #
    # ------------------------------------------------------------------ #

    def push(self, layer: ConfigLayer) -> "Config":
        """Add a layer; higher priority layers shadow lower ones."""
        self._layers.append(layer)
        self._layers.sort(key=lambda l: l.priority, reverse=True)
        return self

    def pop(self, name: str) -> ConfigLayer | None:
        for i, layer in enumerate(self._layers):
            if layer.name == name:
                return self._layers.pop(i)
        return None

    # ------------------------------------------------------------------ #
    # MutableMapping protocol                                              #
    # ------------------------------------------------------------------ #

    def __getitem__(self, key: str) -> Any:
        for layer in self._layers:
            if key in layer:
                return layer[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if not self._layers:
            raise RuntimeError("No layers — push a ConfigLayer first")
        self._layers[0][key] = value

    def __delitem__(self, key: str) -> None:
        for layer in self._layers:
            if key in layer:
                del layer[key]
                return
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for layer in self._layers:
            for key in layer:
                if key not in seen:
                    seen.add(key)
                    yield key

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def get_merged(self, key: str) -> Any:
        """
        Deep-merge a dict-valued key across all layers.
        Higher-priority layer values win on conflicts.
        """
        result: dict = {}
        for layer in reversed(self._layers):
            if key in layer and isinstance(layer[key], dict):
                _deep_merge(result, layer[key])
        return result if result else self.get(key)

    # ------------------------------------------------------------------ #
    # Generators                                                           #
    # ------------------------------------------------------------------ #

    def diff(self, other: "Config") -> Generator[tuple[str, Any, Any], None, None]:
        """
        Yield (key, self_value, other_value) for every key that differs.
        """
        all_keys = set(self) | set(other)
        for key in sorted(all_keys):
            a = self.get(key)
            b = other.get(key)
            if a != b:
                yield key, a, b

    def keys_by_layer(self) -> Generator[tuple[str, str], None, None]:
        """Yield (layer_name, key) for every key in every layer."""
        for layer in self._layers:
            for key in layer:
                yield layer.name, key

    # ------------------------------------------------------------------ #
    # Snapshot / copy                                                      #
    # ------------------------------------------------------------------ #

    def snapshot(self) -> "Config":
        """Deep copy — completely independent config for testing overrides."""
        return copy.deepcopy(self)

    def fork(self, extra: dict[str, Any] | None = None) -> "Config":
        """
        Shallow fork — shares ConfigLayer references (they're effectively
        immutable after construction) but adds an optional override layer.
        """
        new = Config()
        new._layers = list(self._layers)
        if extra:
            new.push(ConfigLayer("fork_override", extra, priority=999))
        return new

    # ------------------------------------------------------------------ #
    # Class-method factories                                               #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_dict(cls, data: dict[str, Any], layer_name: str = "default") -> "Config":
        cfg = cls()
        cfg.push(ConfigLayer(layer_name, data, priority=0))
        return cfg

    @classmethod
    def layered(cls, *layers: tuple[str, dict, int]) -> "Config":
        """Build from (name, data, priority) tuples."""
        cfg = cls()
        for name, data, priority in layers:
            cfg.push(ConfigLayer(name, data, priority))
        return cfg

    @staticmethod
    def validate_keys(config: "Config", required: list[str]) -> list[str]:
        """Return a list of any required keys that are missing."""
        return [k for k in required if k not in config]

    def __repr__(self) -> str:
        return f"Config(layers={[l.name for l in self._layers]}, keys={len(self)})"


def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
