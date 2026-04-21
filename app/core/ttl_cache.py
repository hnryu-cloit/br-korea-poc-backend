from __future__ import annotations

import time
from threading import Lock
from typing import Any


class TTLMemoryCache:
    """프로세스 내 단순 TTL 메모리 캐시"""

    def __init__(self, max_size: int = 256) -> None:
        self.max_size = max(1, max_size)
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = Lock()

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            hit = self._store.get(key)
            if not hit:
                return None
            expires_at, value = hit
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_sec: int) -> None:
        if ttl_sec <= 0:
            return
        now = time.time()
        expires_at = now + ttl_sec
        with self._lock:
            self._store[key] = (expires_at, value)
            if len(self._store) > self.max_size:
                oldest_key = min(self._store.items(), key=lambda item: item[1][0])[0]
                self._store.pop(oldest_key, None)
