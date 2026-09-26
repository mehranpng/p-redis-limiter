"""Atomic Multi-tier Token Bucket Lua Script for Redis."""

TOKEN_BUCKET_LUA = """
local now = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local num_keys = #KEYS

local allowed = 1
local min_remaining = 999999999
local max_retry_after = 0
local max_reset_in = 0

local buckets_tokens = {}
local buckets_last_refill = {}
local capacities = {}
local windows = {}
local ttls = {}

for i = 1, num_keys do
    local offset = 2 + (i - 1) * 3
    local capacity = tonumber(ARGV[offset + 1])
    local window = tonumber(ARGV[offset + 2])
    local ttl_ms = tonumber(ARGV[offset + 3])

    capacities[i] = capacity
    windows[i] = window
    ttls[i] = ttl_ms

    local key = KEYS[i]
    local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
    local tokens = tonumber(bucket[1])
    local last_refill = tonumber(bucket[2])

    if tokens == nil or last_refill == nil then
        tokens = capacity
        last_refill = now
    else
        local time_passed = now - last_refill
        if time_passed > 0 then
            local refill_rate = capacity / window
            tokens = math.min(capacity, tokens + (time_passed * refill_rate))
            last_refill = now
        end
    end

    buckets_tokens[i] = tokens
    buckets_last_refill[i] = last_refill

    if tokens < cost then
        allowed = 0
        local needed = cost - tokens
        local refill_rate = capacity / window
        local retry_after = needed / refill_rate
        if retry_after > max_retry_after then
            max_retry_after = retry_after
        end
    end
end

for i = 1, num_keys do
    local key = KEYS[i]
    local tokens = buckets_tokens[i]
    local last_refill = buckets_last_refill[i]
    local capacity = capacities[i]
    local window = windows[i]
    local ttl_ms = ttls[i]

    if allowed == 1 then
        tokens = tokens - cost
        local rem = math.floor(tokens)
        if rem < min_remaining then
            min_remaining = rem
        end
    else
        min_remaining = 0
    end

    local missing = capacity - tokens
    if missing > 0 then
        local reset_in = missing / (capacity / window)
        if reset_in > max_reset_in then
            max_reset_in = reset_in
        end
    end

    redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(last_refill))
    if ttl_ms > 0 then
        redis.call('PEXPIRE', key, ttl_ms)
    else
        redis.call('PERSIST', key)
    end
end

if min_remaining == 999999999 then
    min_remaining = 0
end

return {allowed, min_remaining, tostring(max_retry_after), tostring(max_reset_in)}
"""
