from __future__ import annotations

import inspect
import math
from typing import Any, Awaitable, Callable, Iterable, Optional, Set, Union

try:
    from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.types import ASGIApp
    STARLETTE_AVAILABLE = True
except ImportError:
    STARLETTE_AVAILABLE = False
    BaseHTTPMiddleware = object
    RequestResponseEndpoint = Any
    Request = Any
    Response = Any
    JSONResponse = Any
    ASGIApp = Any

from p_redis_limiter.limiter import RateLimitResult, RateLimiter
from p_redis_limiter.rate import Rate


def default_client_ip(request: Request) -> str:
    """Extract client IP handling reverse proxy headers (X-Forwarded-For)."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "127.0.0.1"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """FastAPI & Starlette Middleware for Token Bucket Rate Limiting.

    Usage:
        app.add_middleware(
            RateLimitMiddleware,
            redis=r,
            rates=[Rate(5, 1), Rate(40, 60)],
        )
    """

    def __init__(
        self,
        app: ASGIApp,
        redis: Any,
        rates: Optional[Union[Rate, str, tuple[int, Union[int, float]], Iterable[Any]]] = None,
        *,
        requests: Optional[int] = None,
        window: Optional[Union[int, float]] = None,
        prefix: str = "rate",
        ttl: Optional[int] = None,
        identifier: Optional[Callable[[Request], str]] = None,
        error_detail: str = "Too many requests",
        status_code: int = 429,
        set_headers: bool = True,
        exclude_paths: Optional[Iterable[str]] = None,
        on_blocked: Optional[
            Callable[[Request, RateLimitResult], Union[Response, Awaitable[Response]]]
        ] = None,
    ) -> None:
        if not STARLETTE_AVAILABLE:
            raise ImportError(
                "Starlette/FastAPI is required to use RateLimitMiddleware. "
                "Install it with: pip install fastapi"
            )
        super().__init__(app)

        if isinstance(redis, RateLimiter):
            self.limiter = redis
        else:
            self.limiter = RateLimiter(
                redis=redis,
                rates=rates,
                requests=requests,
                window=window,
                prefix=prefix,
                ttl=ttl,
            )

        self.identifier = identifier or default_client_ip
        self.error_detail = error_detail
        self.status_code = status_code
        self.set_headers = set_headers
        self.exclude_paths: Set[str] = set(exclude_paths) if exclude_paths else set()
        self.on_blocked = on_blocked

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if self.exclude_paths and request.url.path in self.exclude_paths:
            return await call_next(request)

        ident = self.identifier(request)
        result = await self.limiter.check_async(ident)

        if not result.allowed:
            if self.on_blocked is not None:
                custom_resp = self.on_blocked(request, result)
                if inspect.isawaitable(custom_resp):
                    return await custom_resp
                return custom_resp

            headers = {}
            if self.set_headers:
                headers = {
                    "Retry-After": str(max(1, int(math.ceil(result.retry_after)))),
                    "X-RateLimit-Limit": str(self.limiter.primary_rate.requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(max(1, int(math.ceil(result.reset_in)))),
                }

            return JSONResponse(
                {"detail": self.error_detail},
                status_code=self.status_code,
                headers=headers if headers else None,
            )

        response = await call_next(request)

        if self.set_headers:
            response.headers["X-RateLimit-Limit"] = str(self.limiter.primary_rate.requests)
            response.headers["X-RateLimit-Remaining"] = str(result.remaining)
            response.headers["X-RateLimit-Reset"] = str(max(1, int(math.ceil(result.reset_in))))

        return response
