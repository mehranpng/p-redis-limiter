import os
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from redis import Redis
from starlette.requests import Request

from p_redis_limiter import Rate, RateLimiter, RateLimitMiddleware

REDIS_SOCKET = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "redis.sock"))


@pytest.fixture
def redis_sync():
    r = Redis(unix_socket_path=REDIS_SOCKET, decode_responses=True)
    r.flushdb()
    yield r
    r.flushdb()
    r.close()


def test_middleware_basic(redis_sync):
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=Rate(3, 10),
    )

    @app.get("/ping")
    def ping():
        return {"ping": "pong"}

    client = TestClient(app)

    # 1st request
    resp1 = client.get("/ping")
    assert resp1.status_code == 200
    assert resp1.json() == {"ping": "pong"}
    assert resp1.headers["X-RateLimit-Limit"] == "3"
    assert resp1.headers["X-RateLimit-Remaining"] == "2"

    # 2nd request
    resp2 = client.get("/ping")
    assert resp2.status_code == 200
    assert resp2.headers["X-RateLimit-Remaining"] == "1"

    # 3rd request
    resp3 = client.get("/ping")
    assert resp3.status_code == 200
    assert resp3.headers["X-RateLimit-Remaining"] == "0"

    # 4th request (rate limited)
    resp4 = client.get("/ping")
    assert resp4.status_code == 429
    assert resp4.json() == {"detail": "Too many requests"}
    assert "Retry-After" in resp4.headers
    assert resp4.headers["X-RateLimit-Remaining"] == "0"


def test_middleware_x_forwarded_for(redis_sync):
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=Rate(1, 10),
    )

    @app.get("/")
    def index():
        return {"ok": True}

    client = TestClient(app)

    # Client IP 1.1.1.1
    r1 = client.get("/", headers={"X-Forwarded-For": "1.1.1.1"})
    assert r1.status_code == 200

    r2 = client.get("/", headers={"X-Forwarded-For": "1.1.1.1"})
    assert r2.status_code == 429

    # Different Client IP 2.2.2.2 has its own quota
    r3 = client.get("/", headers={"X-Forwarded-For": "2.2.2.2, 10.0.0.1"})
    assert r3.status_code == 200


def test_middleware_multiple_rates(redis_sync):
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=[
            Rate(2, 1),   # 2 req per sec
            Rate(5, 60),  # 5 req per min
        ],
    )

    @app.get("/")
    def index():
        return {"ok": True}

    client = TestClient(app)

    # 2 fast requests allowed
    assert client.get("/").status_code == 200
    assert client.get("/").status_code == 200

    # 3rd fast request blocked by 1s tier
    r3 = client.get("/")
    assert r3.status_code == 429


def test_middleware_exclude_paths(redis_sync):
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=Rate(1, 10),
        exclude_paths=["/health"],
    )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api")
    def api():
        return {"data": 123}

    client = TestClient(app)

    # /health is never blocked
    for _ in range(5):
        assert client.get("/health").status_code == 200

    # /api is rate limited
    assert client.get("/api").status_code == 200
    assert client.get("/api").status_code == 429


def test_route_dependency(redis_sync):
    limiter = RateLimiter(redis_sync, 2, 10)
    app = FastAPI()

    @app.get("/limited", dependencies=[Depends(limiter.as_dependency())])
    def limited_route():
        return {"status": "success"}

    client = TestClient(app)

    assert client.get("/limited").status_code == 200
    assert client.get("/limited").status_code == 200
    resp = client.get("/limited")
    assert resp.status_code == 429
    assert resp.json() == {"detail": "Too many requests"}


def test_middleware_custom_ttl(redis_sync):
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=Rate(5, 10),
        ttl=250,
    )

    @app.get("/")
    def index():
        return {"ok": True}

    client = TestClient(app)
    client.get("/")
    ttl = redis_sync.ttl("rate:testclient")
    assert 245 <= ttl <= 250


def test_middleware_custom_on_blocked(redis_sync):
    from starlette.responses import JSONResponse

    def my_blocked_handler(request: Request, result):
        return JSONResponse(
            {"error": "rate_limited", "retry_in": result.retry_after},
            status_code=429,
        )

    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        redis=redis_sync,
        rates=Rate(1, 10),
        on_blocked=my_blocked_handler,
    )

    @app.get("/")
    def index():
        return {"ok": True}

    client = TestClient(app)
    client.get("/")  # 1st: ok
    r2 = client.get("/")  # 2nd: blocked
    assert r2.status_code == 429
    assert r2.json()["error"] == "rate_limited"
    assert "retry_in" in r2.json()
