local rate_limit_key = KEYS[1]
local timestamp_key = KEYS[1] .. ':ts'

local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = 1

-- Fetch current state
local last_tokens = tonumber(redis.call('get', rate_limit_key))
local last_refreshed = tonumber(redis.call('get', timestamp_key))

-- Initialize if it doesn't exist
if last_tokens == nil then
    last_tokens = capacity
    last_refreshed = now
end

-- Calculate tokens to add based on elapsed time
local time_passed = math.max(0, now - last_refreshed)
local new_tokens = math.min(capacity, last_tokens + (time_passed * refill_rate))

-- Check capacity and update
if new_tokens >= requested then
    new_tokens = new_tokens - requested
    -- Set a 120-second TTL (Time To Live) to clean up inactive domains
    redis.call('setex', rate_limit_key, 120, new_tokens)
    redis.call('setex', timestamp_key, 120, now)
    return 1 -- Allowed
else
    -- Update TTL but don't change the timestamp since we didn't consume
    redis.call('setex', rate_limit_key, 120, new_tokens)
    redis.call('setex', timestamp_key, 120, last_refreshed)
    return 0 -- Rate Limited
end