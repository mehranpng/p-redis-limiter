# p-redis-limiter

Redis token bucket rate limiter for Python and FastAPI.

## Installation

```bash
pip install p-redis-limiter
```

## Usage

### 1. Global Middleware (All Endpoints)

Apply a rate limit across all endpoints:

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
def index():
    return {"ok": True}
```

### 2. Specific Endpoint Only

To rate limit a specific route instead of the whole application, use `RateLimiter` with `Depends`:

```python
from fastapi import FastAPI, Depends
from redis import Redis
from p_redis_limiter import RateLimiter, Rate

app = FastAPI()
r = Redis(host="localhost", port=6379, decode_responses=True)

# 5 requests per 60 seconds for login only
login_limiter = RateLimiter(r, Rate(5, 60), prefix="rate:login")

@app.get("/")
def home():
    return {"message": "unlimited"}

@app.post("/login", dependencies=[Depends(login_limiter.as_dependency())])
def login():
    return {"message": "login successful"}
```

### 3. Multiple Rate Limits

You can define multiple rules (e.g. 5 req/sec burst limit and 100 req/min). If one rule fails, tokens are not deducted from the others:

```python
rates = [
    Rate(5, 1),    # 5 requests per 1 second
    Rate(100, 60), # 100 requests per 60 seconds
]

# Or string shorthand:
rates = ["5/s", "100/m"]
```

### 4. Cache TTL

By default, Redis keys expire automatically after `max(window * 2, 60)` seconds. You can specify a custom TTL:

```python
# Per rate rule:
Rate(40, 60, ttl=120)

# Disable TTL (persist in Redis forever):
Rate(40, 60, ttl=-1)

# Or globally on middleware / limiter:
RateLimitMiddleware(redis=r, rates=Rate(40, 60), ttl=120)
```

### 5. Manual Usage (Specify IP Yourself)

If you prefer to get the IP yourself, simply pass your IP variable directly:

```python
from p_redis_limiter import RateLimiter, Rate

limiter = RateLimiter(r, Rate(5, 60))

@app.post("/login")
def login(request: Request):
    user_ip = get_my_ip(request)  # your own IP variable

    # Simple boolean check:
    if not limiter.is_allowed(user_ip):
        return JSONResponse({"detail": "Too many requests"}, status_code=429)

    return {"ok": True}
```

Or get full details (`remaining`, `retry_after`):

```python
result = limiter.check(user_ip)
if not result.allowed:
    print(f"Blocked! Retry after {result.retry_after}s")
```

### 6. Redis Failure & Fallback Handling

Configure what happens if Redis becomes unreachable:

* **Default (`on_redis_error="raise"`):** Raises the Redis connection exception (unhandled errors return HTTP 500).
* **Fail-Open (`on_redis_error="allow"` or `"pass"`):** Bypasses rate limiting and allows requests through so your service stays online.
* **In-Memory Fallback (`on_redis_error="local"`):** Seamlessly applies an in-memory token bucket rate limit until Redis is back up. Once Redis is reachable again, it automatically recovers and switches back to Redis.

```python
# Option A: Allow requests through if Redis goes down (fail-open)
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates="40/m",
    on_redis_error="allow",
)

# Option B: Fallback to in-memory rate limiting using the same rate rules
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates="40/m",
    on_redis_error="local",
)

# Option C: Fallback to in-memory rate limiting with a specific/stricter rate
app.add_middleware(
    RateLimitMiddleware,
    redis=r,
    rates="40/m",
    on_redis_error="local",
    fallback_rate="10/m",
)

# Also works with RateLimiter / Depends:
login_limiter = RateLimiter(
    r,
    "5/m",
    on_redis_error="local",
    fallback_rate="2/m",
)
```

## Options

| Parameter | Type | Default | Description |
|---|---|---|---|
| `redis` | `Redis` | Required | Redis client instance (`redis.Redis` or `redis.asyncio.Redis`) |
| `rates` | `Rate` / `list` / `str` | Required | Rate rules, e.g. `Rate(40, 60)` or `["5/s", "100/m"]` |
| `ttl` | `int` | Auto | Custom Redis key TTL in seconds |
| `prefix` | `str` | `"rate"` | Prefix for Redis keys |
| `identifier` | `Callable` / `str` | Client IP | Custom function or header name (auto-detects Cloudflare, Nginx, ALB, direct IP) |
| `exclude_paths` | `list[str]` | `None` | List of paths to exclude from rate limiting |
| `error_detail` | `str` | `"Too many requests"` | Response detail message on 429 |
| `on_redis_error` | `str` / `Rate` | `"raise"` | Behavior when Redis fails: `"raise"`, `"allow"` (or `"pass"`), or `"local"` |
| `fallback_rate` | `Rate` / `str` / `list` | `None` | Custom rate limit rule to use when falling back to `"local"` mode |
| `redis_retry_interval` | `float` | `1.0` | Seconds to wait before re-probing Redis after a failure |
