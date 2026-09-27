from __future__ import annotations

import math
import threading
import time
from typing import Any, Iterable, List, Optional, Union

from p_redis_limiter.rate import Rate


class LocalTokenBucketLimiter:
    """Thread-safe In-Memory Token Bucket Rate Limiter.

    Used as a fallback mechanism when Redis is unreachable, or as a standalone
    in-memory rate limiter. Supports single and multi-tier rate rules with the
    exact same atomic semantics as the Redis Lua script.
    """

    def __init__(
        self,
        rates: Optional[Union[Rate, str, tuple[int, Union[int, float]], Iterable[Any]]] = None,
        requests: Optional[int] = None,
        window: Optional[Union[int, float]] = None,
        max_entries: int = 10000,
    ) -> None:
        self.max_entries = max_entries
        self._lock = threading.Lock()

        parsed_rates: List[Rate] = []
        if rates is not None:
            if isinstance(rates, (Rate, str)):
                parsed_rates.append(Rate.of(rates))
            elif isinstance(rates, tuple) and len(rates) >= 2 and isinstance(rates[0], int):
                parsed_rates.append(Rate.of(rates))
            elif isinstance(rates, Iterable):
                for r in rates:
                    parsed_rates.append(Rate.of(r))
        elif requests is not None and window is not None:
            parsed_rates.append(Rate(requests=requests, window=float(window)))

        if not parsed_rates:
            raise ValueError("At least one rate must be provided for LocalTokenBucketLimiter.")

        self.rates: tuple[Rate, ...] = tuple(parsed_rates)
        self.is_multi: bool = len(self.rates) > 1
        self._max_window = max(r.window for r in self.rates)

        self._buckets: dict[str, dict[str, tuple[float, float]]] = {}
        self._last_access: dict[str, float] = {}

    @property
    def primary_rate(self) -> Rate:
        return self.rates[0]

    def _cleanup_unlocked(self, now: float) -> None:
        """Evict stale or excess entries. Must be called while holding self._lock."""
        expiry_threshold = max(self._max_window * 2.0, 60.0)
        stale_keys = [
            k for k, last_ts in self._last_access.items()
            if (now - last_ts) > expiry_threshold
        ]
        for k in stale_keys:
            self._buckets.pop(k, None)
            self._last_access.pop(k, None)

        if len(self._buckets) > self.max_entries:
            sorted_keys = sorted(self._last_access.items(), key=lambda item: item[1])
            excess_count = len(self._buckets) - self.max_entries
            for k, _ in sorted_keys[:excess_count]:
                self._buckets.pop(k, None)
                self._last_access.pop(k, None)

    def check(self, identifier: str, cost: int = 1, now: Optional[float] = None) -> Any:
        from p_redis_limiter.limiter import RateLimitResult

        if cost <= 0:
            raise ValueError("cost must be a positive integer >= 1")

        if now is None:
            now = time.time()

        with self._lock:
            if len(self._buckets) >= self.max_entries:
                self._cleanup_unlocked(now)

            user_buckets = self._buckets.setdefault(identifier, {})
            self._last_access[identifier] = now

            allowed = True
            max_retry_after = 0.0
            max_reset_in = 0.0
            min_remaining = 999999999

            temp_states: list[tuple[Rate, str, float, float]] = []

            for r in self.rates:
                tag = r.window_tag
                capacity = float(r.requests)
                window = float(r.window)

                if tag in user_buckets:
                    tokens, last_refill = user_buckets[tag]
                    time_passed = max(0.0, now - last_refill)
                    if time_passed > 0:
                        refill_rate = capacity / window
                        tokens = min(capacity, tokens + (time_passed * refill_rate))
                        last_refill = now
                else:
                    tokens = capacity
                    last_refill = now

                if tokens < cost:
                    allowed = False
                    needed = cost - tokens
                    refill_rate = capacity / window
                    retry_after = needed / refill_rate
                    if retry_after > max_retry_after:
                        max_retry_after = retry_after

                temp_states.append((r, tag, tokens, last_refill))

            for r, tag, tokens, last_refill in temp_states:
                capacity = float(r.requests)
                window = float(r.window)

                if allowed:
                    tokens = tokens - cost
                    rem = int(math.floor(tokens))
                    if rem < min_remaining:
                        min_remaining = rem
                else:
                    min_remaining = 0

                missing = capacity - tokens
                if missing > 0:
                    reset_in = missing / (capacity / window)
                    if reset_in > max_reset_in:
                        max_reset_in = reset_in

                user_buckets[tag] = (tokens, last_refill)

            if min_remaining == 999999999:
                min_remaining = 0

            return RateLimitResult(
                allowed=allowed,
                remaining=min_remaining,
                retry_after=round(max_retry_after, 4),
                reset_in=round(max_reset_in, 4),
            )

    def is_allowed(self, identifier: str, cost: int = 1) -> bool:
        return self.check(identifier, cost).allowed

    def reset(self, identifier: Optional[str] = None) -> None:
        """Reset buckets for a specific identifier or all identifiers."""
        with self._lock:
            if identifier is not None:
                self._buckets.pop(identifier, None)
                self._last_access.pop(identifier, None)
            else:
                self._buckets.clear()
                self._last_access.clear()
