import os
import time
import pytest
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from p_redis_limiter import Rate, RateLimiter, RateLimitResult

REDIS_SOCKET = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "redis.sock"))


@pytest.fixture
def redis_sync():
    r = Redis(unix_socket_path=REDIS_SOCKET, decode_responses=True)
    r.flushdb()
    yield r
    r.flushdb()
    r.close()


import pytest_asyncio

@pytest_asyncio.fixture
async def redis_async():
    r = AsyncRedis(unix_socket_path=REDIS_SOCKET, decode_responses=True)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()


def test_single_rate_basic(redis_sync):
    limiter = RateLimiter(redis_sync, 3, 2.0)  # 3 req per 2 sec
    key = "user_1"

    # Request 1
    res1 = limiter.check(key)
    assert res1.allowed is True
    assert res1.remaining == 2
    assert res1.retry_after == 0.0

    # Request 2
    res2 = limiter.check(key)
    assert res2.allowed is True
    assert res2.remaining == 1

    # Request 3
    res3 = limiter.check(key)
    assert res3.allowed is True
    assert res3.remaining == 0

    # Request 4 (exceeded)
    res4 = limiter.check(key)
    assert res4.allowed is False
    assert res4.remaining == 0
    assert res4.retry_after > 0

    # Wait for refill (0.7s should refill at least 1 token since rate is 3/2 = 1.5 tokens/s)
    time.sleep(0.75)
    res5 = limiter.check(key)
    assert res5.allowed is True
    assert res5.remaining >= 0


def test_ttl_set_and_custom(redis_sync):
    # Test default TTL: max(ceil(10 * 2), 60) -> 60
    limiter_default = RateLimiter(redis_sync, 10, 10)
    limiter_default.check("user_default_ttl")
    ttl = redis_sync.ttl("rate:user_default_ttl")
    assert 55 <= ttl <= 60

    # Test custom TTL on Rate
    limiter_custom_rate = RateLimiter(redis_sync, Rate(10, 10, ttl=180))
    limiter_custom_rate.check("user_custom_rate_ttl")
    ttl = redis_sync.ttl("rate:user_custom_rate_ttl")
    assert 175 <= ttl <= 180

    # Test custom TTL on RateLimiter
    limiter_custom_global = RateLimiter(redis_sync, 10, 10, ttl=300)
    limiter_custom_global.check("user_custom_global_ttl")
    ttl = redis_sync.ttl("rate:user_custom_global_ttl")
    assert 295 <= ttl <= 300


def test_multi_rate_atomicity(redis_sync):
    # 2 requests per 1 second (burst) AND 4 requests per 10 seconds
    rates = [Rate(2, 1.0), Rate(4, 10.0)]
    limiter = RateLimiter(redis_sync, rates=rates)
    key = "user_multi"

    # Req 1: Allowed (burst: 1 left, 10s: 3 left)
    r1 = limiter.check(key)
    assert r1.allowed is True
    assert r1.remaining == 1

    # Req 2: Allowed (burst: 0 left, 10s: 2 left)
    r2 = limiter.check(key)
    assert r2.allowed is True
    assert r2.remaining == 0

    # Req 3: Blocked by burst limit!
    r3 = limiter.check(key)
    assert r3.allowed is False
    assert r3.retry_after > 0

    # CRITICAL ATOMICITY CHECK:
    # Because req 3 was blocked by tier 1, tier 2 tokens MUST NOT have been consumed!
    key_10s = f"rate:{key}:10s"
    bucket_10s = redis_sync.hgetall(key_10s)
    tokens_10s = float(bucket_10s["tokens"])
    # 2 tokens were consumed out of 4, so ~2 tokens must remain, NOT 1
    assert tokens_10s >= 1.95


@pytest.mark.asyncio
async def test_async_redis_limiter(redis_async):
    limiter = RateLimiter(redis_async, 2, 1.0)
    key = "user_async"

    r1 = await limiter.check_async(key)
    assert r1.allowed is True
    assert r1.remaining == 1

    r2 = await limiter.check_async(key)
    assert r2.allowed is True
    assert r2.remaining == 0

    r3 = await limiter.check_async(key)
    assert r3.allowed is False


@pytest.mark.asyncio
async def test_sync_client_in_async_context(redis_sync):
    # Tests that a synchronous redis client can be safely used with check_async
    limiter = RateLimiter(redis_sync, 2, 1.0)
    key = "user_sync_in_async"

    r1 = await limiter.check_async(key)
    assert r1.allowed is True

    r2 = await limiter.check_async(key)
    assert r2.allowed is True

    r3 = await limiter.check_async(key)
    assert r3.allowed is False


def test_sync_check_with_async_client_fails(redis_async):
    limiter = RateLimiter(redis_async, 2, 1.0)
    with pytest.raises(RuntimeError, match="Cannot use synchronous check"):
        limiter.check("user_err")


def test_flexible_initialization(redis_sync):
    # From string
    l1 = RateLimiter(redis_sync, "5/s")
    assert l1.primary_rate.requests == 5
    assert l1.primary_rate.window == 1.0

    # From tuple
    l2 = RateLimiter(redis_sync, (10, 60))
    assert l2.primary_rate.requests == 10
    assert l2.primary_rate.window == 60.0

    # From kwargs
    l3 = RateLimiter(redis_sync, requests=20, window=30)
    assert l3.primary_rate.requests == 20
    assert l3.primary_rate.window == 30.0

    # Multiple string rates
    l4 = RateLimiter(redis_sync, rates=["5/s", "100/m"])
    assert len(l4.rates) == 2
    assert l4.rates[0].requests == 5
    assert l4.rates[1].requests == 100


def test_custom_key_builder(redis_sync):
    def custom_builder(prefix, ident, rate, is_multi):
        return f"custom:{ident}:{rate.requests}_{int(rate.window)}"

    limiter = RateLimiter(
        redis_sync,
        Rate(5, 10),
        key_builder=custom_builder,
    )
    limiter.check("abc")
    assert redis_sync.exists("custom:abc:5_10") == 1
