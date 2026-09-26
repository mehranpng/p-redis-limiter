# p-redis-limiter

A lightweight, fast, and atomic **Token Bucket Rate Limiter** for Python and FastAPI powered by Redis.

## Features
- **Atomic Execution**: All token bucket calculations and multi-tier checks run atomically in a single Redis Lua script.
- **Multiple Rate Limits**: Define simultaneous tiers (e.g., 5 req/sec burst limit + 100 req/min). If one fails, others are not decremented.
- **Automatic & Custom TTL**: Redis keys automatically expire when idle. Set custom TTL globally or per rate limit.
- **Zero-Boilerplate FastAPI**: Drop-in `RateLimitMiddleware` handles headers (`X-RateLimit-*`, `Retry-After`) and IP detection.
- **Sync & Async Redis**: Supports `redis.Redis` and `redis.asyncio.Redis`.

---

## Installation

```bash
pip install git+https://github.com/mehranpng/p-redis-limiter.git
```

---

## Quickstart (FastAPI)

```python
from fastapi import FastAPI
from redis import Redis
from p_redis_limiter import RateLimitMiddleware, Rate

app = FastAPI()
r = Redis(host="localhost", port=6379, decode_responses=True)

# 40 requests per 60 seconds
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates=Rate(40, 60),
)

@app.get("/")
def root():
    return {"ping": "pong"}
```

---

## Multiple Rate Limits

Enforce both short-term burst protection and long-term quotas:

```python
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates=[
        Rate(5, 1),     # Max 5 requests per 1 second
        Rate(100, 60),  # Max 100 requests per 60 seconds
    ],
)

# Shorthand string syntax is also supported:
# rates=["5/s", "100/m"]
```

---

## Setting TTL (Cache Expiration)

By default, TTL is automatically set to `max(window * 2, 60)` seconds so inactive keys clean up from Redis. You can set a custom TTL:

```python
# 1. Custom TTL per rate:
Rate(40, 60, ttl=120)  # expires in 120s

# 2. Custom TTL globally on middleware:
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates=Rate(40, 60),
    ttl=300, # 5 minutes
)
```

---

## Standalone Usage (Without FastAPI)

```python
from redis import Redis
from p_redis_limiter import RateLimiter, Rate

r = Redis(host="localhost", port=6379, decode_responses=True)

limiter = RateLimiter(r, rates=[Rate(5, 1), Rate(100, 60)])

result = limiter.check("user:123")
if not result.allowed:
    print(f"Blocked! Try again in {result.retry_after} seconds.")
else:
    print(f"Allowed! Tokens remaining: {result.remaining}")

# For async Redis (redis.asyncio.Redis):
# result = await limiter.check_async("user:123")
```

---

## FastAPI Route-Specific Limiting

```python
from fastapi import Depends

limiter = RateLimiter(r, "3/m")

@app.post("/login", dependencies=[Depends(limiter.as_dependency())])
def login():
    return {"status": "ok"}
```

---

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `redis` | `Redis` / `AsyncRedis` | *required* | Redis client instance |
| `rates` | `Rate` / `list[Rate]` / `str` | *required* | Rate limits (e.g. `Rate(40, 60)` or `["5/s", "100/m"]`) |
| `ttl` | `int` | `auto` | Custom TTL in seconds for Redis keys |
| `prefix` | `str` | `"rate"` | Prefix for Redis keys |
| `identifier` | `Callable[[Request], str]` | Client IP | Custom function to identify client (IP, user ID, API key) |
| `exclude_paths` | `list[str]` | `None` | Endpoints exempt from rate limiting (e.g. `["/health"]`) |
| `error_detail` | `str` | `"Too many requests"` | Detail message in 429 response |
