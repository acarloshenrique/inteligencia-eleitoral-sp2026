from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class _CacheItem(Generic[V]):
    value: V
    created_at: datetime


class TimedLruCache(Generic[K, V]):
    def __init__(self, maxsize: int = 256, ttl_seconds: int = 600):
        self._maxsize = maxsize
        self._ttl = timedelta(seconds=max(1, ttl_seconds))
        self._items: OrderedDict[K, _CacheItem[V]] = OrderedDict()

    def _is_expired(self, item: _CacheItem[V]) -> bool:
        return datetime.now(UTC) - item.created_at > self._ttl

    def get(self, key: K) -> V | None:
        item = self._items.get(key)
        if item is None:
            return None
        if self._is_expired(item):
            self._items.pop(key, None)
            return None
        self._items.move_to_end(key)
        return item.value

    def set(self, key: K, value: V) -> None:
        self._items[key] = _CacheItem(value=value, created_at=datetime.now(UTC))
        self._items.move_to_end(key)
        while len(self._items) > self._maxsize:
            self._items.popitem(last=False)

    def clear(self) -> None:
        self._items.clear()
