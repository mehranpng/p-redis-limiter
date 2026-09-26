import pytest
from p_redis_limiter.rate import Rate


def test_rate_init():
    rate = Rate(requests=40, window=60)
    assert rate.requests == 40
    assert rate.window == 60.0
    assert rate.ttl is None
    # Default TTL: max(ceil(60 * 2), 60) -> 120
    assert rate.effective_ttl() == 120


def test_rate_custom_ttl():
    rate = Rate(requests=40, window=60, ttl=300)
    assert rate.ttl == 300
    assert rate.effective_ttl() == 300
    assert rate.effective_ttl(default_ttl=500) == 300


def test_rate_default_ttl_override():
    rate = Rate(requests=10, window=1)
    # Default fallback: max(ceil(1 * 2), 60) = 60
    assert rate.effective_ttl() == 60
    # Overridden by limiter global default_ttl
    assert rate.effective_ttl(default_ttl=30) == 30


def test_rate_invalid_args():
    with pytest.raises(ValueError):
        Rate(requests=0, window=60)
    with pytest.raises(ValueError):
        Rate(requests=-5, window=60)
    with pytest.raises(ValueError):
        Rate(requests=10, window=0)
    with pytest.raises(ValueError):
        Rate(requests=10, window=-1)
    with pytest.raises(ValueError):
        Rate(requests=10, window=10, ttl=0)


@pytest.mark.parametrize(
    "input_str,expected_req,expected_window",
    [
        ("5/s", 5, 1.0),
        ("5/sec", 5, 1.0),
        ("5/second", 5, 1.0),
        ("5/1s", 5, 1.0),
        ("40/m", 40, 60.0),
        ("40/min", 40, 60.0),
        ("40/minute", 40, 60.0),
        ("40/2m", 40, 120.0),
        ("100/h", 100, 3600.0),
        ("100/hour", 100, 3600.0),
        ("1000/d", 1000, 86400.0),
        ("1000/day", 1000, 86400.0),
        ("50/10s", 50, 10.0),
        ("20/0.5s", 20, 0.5),
    ],
)
def test_rate_parse(input_str, expected_req, expected_window):
    rate = Rate.parse(input_str)
    assert rate.requests == expected_req
    assert rate.window == expected_window


def test_rate_parse_invalid():
    with pytest.raises(ValueError):
        Rate.parse("invalid")
    with pytest.raises(ValueError):
        Rate.parse("5/unknownunit")


def test_rate_of_helper():
    r1 = Rate.of("5/s")
    assert r1.requests == 5
    assert r1.window == 1.0

    r2 = Rate.of((40, 60))
    assert r2.requests == 40
    assert r2.window == 60.0

    r3 = Rate.of(r1)
    assert r3 is r1

    r4 = Rate.of((40, 60, 180))
    assert r4.requests == 40
    assert r4.ttl == 180

    with pytest.raises(TypeError):
        Rate.of(12345)
