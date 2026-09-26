from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Optional, Union

_TIME_UNITS = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}

_RATE_REGEX = re.compile(
    r"^\s*(\d+)\s*\/\s*(?:(\d+(?:\.\d+)?)\s*)?([a-zA-Z]+)?\s*$"
)


@dataclass(frozen=True)
class Rate:
    """Represents a rate limit rule: allowed `requests` within `window` seconds.

    Args:
        requests: Number of requests allowed within the window.
        window: Window duration in seconds (int or float).
        ttl: Optional custom TTL in seconds for Redis cache.
             If not set, defaults to max(ceil(window * 2), 60).
    """

    requests: int
    window: float
    ttl: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.requests, int) or self.requests <= 0:
            raise ValueError(f"requests must be a positive integer > 0, got: {self.requests}")
        if self.window <= 0:
            raise ValueError(f"window must be a positive number > 0, got: {self.window}")
        if self.ttl is not None and self.ttl <= 0:
            raise ValueError(f"ttl must be a positive integer if specified, got: {self.ttl}")

    def effective_ttl(self, default_ttl: Optional[int] = None) -> int:
        """Returns the TTL in seconds to use in Redis."""
        if self.ttl is not None:
            return int(self.ttl)
        if default_ttl is not None:
            return int(default_ttl)
        return max(int(math.ceil(self.window * 2)), 60)

    @property
    def window_tag(self) -> str:
        """Compact string representation of the window duration for key suffix."""
        if float(self.window).is_integer():
            return f"{int(self.window)}s"
        return f"{self.window}s"

    @classmethod
    def parse(cls, rate_str: str, ttl: Optional[int] = None) -> Rate:
        """Parse human-friendly rate string such as '5/s', '40/minute', '100/10s'.

        Examples:
            Rate.parse("5/s") -> Rate(requests=5, window=1.0)
            Rate.parse("40/minute") -> Rate(requests=40, window=60.0)
            Rate.parse("10/2m") -> Rate(requests=10, window=120.0)
            Rate.parse("100/30s", ttl=120) -> Rate(requests=100, window=30.0, ttl=120)
        """
        match = _RATE_REGEX.match(rate_str.strip())
        if not match:
            raise ValueError(
                f"Invalid rate limit string: '{rate_str}'. "
                "Expected format like '5/s', '40/minute', '100/30s', etc."
            )

        req_str, mult_str, unit_str = match.groups()
        requests = int(req_str)

        multiplier = float(mult_str) if mult_str else 1.0

        if unit_str:
            unit_lower = unit_str.lower()
            if unit_lower not in _TIME_UNITS:
                valid = ", ".join(_TIME_UNITS.keys())
                raise ValueError(
                    f"Unknown time unit '{unit_str}' in rate '{rate_str}'. Valid units: {valid}"
                )
            base_seconds = _TIME_UNITS[unit_lower]
        else:
            base_seconds = 1.0

        window = multiplier * base_seconds
        return cls(requests=requests, window=window, ttl=ttl)

    @classmethod
    def of(cls, value: Union[Rate, str, tuple[int, Union[int, float]], list[Any]], default_ttl: Optional[int] = None) -> Rate:
        """Convert various input types into a Rate instance."""
        if isinstance(value, Rate):
            if value.ttl is None and default_ttl is not None:
                return cls(requests=value.requests, window=value.window, ttl=default_ttl)
            return value
        if isinstance(value, str):
            return cls.parse(value, ttl=default_ttl)
        if isinstance(value, (tuple, list)) and len(value) >= 2:
            custom_ttl = value[2] if len(value) > 2 else default_ttl
            return cls(requests=int(value[0]), window=float(value[1]), ttl=custom_ttl)
        raise TypeError(
            f"Cannot convert {type(value).__name__} to Rate. "
            "Pass a Rate instance, a string (e.g. '40/m'), or a (requests, window) tuple."
        )

    def __str__(self) -> str:
        return f"{self.requests}/{self.window_tag}"
