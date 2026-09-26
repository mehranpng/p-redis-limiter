from __future__ import annotations

import asyncio
import inspect
import math
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Union

from p_redis_limiter.lua import TOKEN_BUCKET_LUA
from p_redis_limiter.rate import Rate

try:
    from starlette.requests import Request
    from starlette.exceptions import HTTPException
except ImportError:
    Request = Any
    HTTPException = Exception


@dataclass(frozen=True)
class RateLimitResult:
    """Result of a rate limit check.

    Attributes:
        allowed: True if request is allowed, False if rate limited.
        remaining: Remaining tokens in the bucket (minimum across all tiers).
        retry_after: Seconds to wait before retrying (0.0 if allowed).
        reset_in: Seconds until all buckets are fully refilled to capacity.
    """

    allowed: bool
    remaining: int
    retry_after: float
    reset_in: float


def _default_key_builder(prefix: str, identifier: str, rate: Rate, is_multi: bool) -> str:
    if not is_multi:
        return f"{prefix}:{identifier}"
    return f"{prefix}:{identifier}:{rate.window_tag}"


def _is_async_redis_client(client: Any) -> bool:
    try:
        from redis.asyncio import Redis as AsyncRedis, RedisCluster as AsyncRedisCluster
        if isinstance(client, (AsyncRedis, AsyncRedisCluster)):
            return True
    except ImportError:
        pass

    module_name = getattr(client.__class__, "__module__", "")
    if "asyncio" in module_name:
        return True

    for attr in ("execute_command", "eval", "evalsha"):
        func = getattr(client, attr, None)
        if func and inspect.iscoroutinefunction(func):
            return True

    return False


class RateLimiter:
    """Atomic Token Bucket Rate Limiter powered by Redis.

    Supports single or multiple rate limits, automatic/custom TTL, and both
    synchronous and asynchronous Redis clients.

    Usage examples:
        limiter = RateLimiter(r, 40, 60)
        limiter = RateLimiter(r, Rate(40, 60))
        limiter = RateLimiter(r, "40/m")

        limiter = RateLimiter(r, rates=[Rate(5, 1), Rate(100, 60)])
        limiter = RateLimiter(r, rates=["5/s", "100/m"])
    """

    def __init__(
        self,
        redis: Any,
        *args: Any,
        rates: Optional[Union[Rate, str, tuple[int, Union[int, float]], Iterable[Any]]] = None,
        requests: Optional[int] = None,
        window: Optional[Union[int, float]] = None,
        prefix: str = "rate",
        ttl: Optional[int] = None,
        key_builder: Optional[Callable[[str, str, Rate, bool], str]] = None,
    ) -> None:
        self.redis = redis
        self.prefix = prefix
        self.ttl = ttl
        self._key_builder = key_builder or _default_key_builder
        self._is_async = _is_async_redis_client(redis)

        parsed_rates: List[Rate] = []

        if len(args) == 2 and isinstance(args[0], int) and isinstance(args[1], (int, float)):
            parsed_rates.append(Rate(requests=args[0], window=float(args[1]), ttl=ttl))
        elif len(args) == 1:
            rates = args[0]

        if rates is not None:
            if isinstance(rates, (Rate, str, tuple)):
                parsed_rates.append(Rate.of(rates, default_ttl=ttl))
            elif isinstance(rates, Iterable):
                for r in rates:
                    parsed_rates.append(Rate.of(r, default_ttl=ttl))
        elif requests is not None and window is not None:
            parsed_rates.append(Rate(requests=requests, window=float(window), ttl=ttl))

        if not parsed_rates:
            raise ValueError(
                "At least one rate must be provided. "
                "E.g.: RateLimiter(r, 40, 60) or RateLimiter(r, rates=[Rate(5, 1), Rate(100, 60)])"
            )

        self.rates: tuple[Rate, ...] = tuple(parsed_rates)
        self.is_multi: bool = len(self.rates) > 1

        if hasattr(self.redis, "register_script"):
            self._script = self.redis.register_script(TOKEN_BUCKET_LUA)
        else:
            self._script = None

    @property
    def primary_rate(self) -> Rate:
        """The primary (first) rate configured."""
        return self.rates[0]

    def get_key(self, identifier: str, rate: Rate) -> str:
        """Build the Redis key for an identifier and rate tier."""
        return self._key_builder(self.prefix, identifier, rate, self.is_multi)

    def _prepare_call(self, identifier: str, cost: int) -> tuple[list[str], list[Any]]:
        if cost <= 0:
            raise ValueError("cost must be a positive integer >= 1")

        now = time.time()
        keys = [self.get_key(identifier, r) for r in self.rates]

        args: list[Any] = [now, cost]
        for r in self.rates:
            effective_ttl_s = r.effective_ttl(self.ttl)
            ttl_ms = int(effective_ttl_s * 1000)
            args.extend([r.requests, r.window, ttl_ms])

        return keys, args

    def _parse_result(self, raw: Any) -> RateLimitResult:
        allowed = bool(raw[0])
        remaining = int(raw[1])
        retry_after = round(float(raw[2]), 4)
        reset_in = round(float(raw[3]), 4)

        return RateLimitResult(
            allowed=allowed,
            remaining=remaining,
            retry_after=retry_after,
            reset_in=reset_in,
        )

    def check(self, identifier: str, cost: int = 1) -> RateLimitResult:
        """Check and consume rate limit synchronously.

        Raises RuntimeError if initialized with an async Redis client.
        """
        if self._is_async:
            raise RuntimeError(
                "Cannot use synchronous check() with an async Redis client. "
                "Use 'await limiter.check_async(...)' instead."
            )

        keys, args = self._prepare_call(identifier, cost)

        if self._script is not None:
            raw = self._script(keys=keys, args=args)
        else:
            raw = self.redis.eval(TOKEN_BUCKET_LUA, len(keys), *keys, *args)

        return self._parse_result(raw)

    async def check_async(self, identifier: str, cost: int = 1) -> RateLimitResult:
        """Check and consume rate limit asynchronously.

        Works with both async Redis clients (native await) and sync Redis clients
        (dispatched to thread pool).
        """
        keys, args = self._prepare_call(identifier, cost)

        if self._is_async:
            if self._script is not None:
                raw = await self._script(keys=keys, args=args)
            else:
                raw = await self.redis.eval(TOKEN_BUCKET_LUA, len(keys), *keys, *args)
        else:
            if self._script is not None:
                raw = await asyncio.to_thread(self._script, keys=keys, args=args)
            else:
                raw = await asyncio.to_thread(
                    self.redis.eval, TOKEN_BUCKET_LUA, len(keys), *keys, *args
                )

        return self._parse_result(raw)

    def is_allowed(self, identifier: str, cost: int = 1) -> bool:
        """Convenience method returning True if request is allowed, False otherwise."""
        return self.check(identifier, cost).allowed

    async def is_allowed_async(self, identifier: str, cost: int = 1) -> bool:
        """Async convenience method returning True if request is allowed, False otherwise."""
        res = await self.check_async(identifier, cost)
        return res.allowed

    def as_dependency(
        self,
        cost: int = 1,
        identifier_func: Optional[Callable[[Any], str]] = None,
        error_detail: str = "Too many requests",
    ):
        """Create a FastAPI/Starlette route dependency.

        Usage:
            @app.get("/items", dependencies=[Depends(limiter.as_dependency())])
            def get_items():
                ...
        """
        from p_redis_limiter.middleware import default_client_ip

        ident_fn = identifier_func or default_client_ip

        async def _rate_limit_dependency(request: Request):
            ident = ident_fn(request)
            result = await self.check_async(ident, cost=cost)

            if not result.allowed:
                headers = {
                    "Retry-After": str(max(1, int(math.ceil(result.retry_after)))),
                    "X-RateLimit-Limit": str(self.primary_rate.requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(max(1, int(math.ceil(result.reset_in)))),
                }
                raise HTTPException(
                    status_code=429,
                    detail=error_detail,
                    headers=headers,
                )
            return result

        return _rate_limit_dependency
