package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenBucket implements a distributed token bucket rate limiter using Redis.
type TokenBucket struct {
	client   *redis.Client
	capacity int
	refill   float64 // tokens per second
	ttl      time.Duration
}

// NewTokenBucket constructs a bucket with the provided capacity/refill.
func NewTokenBucket(client *redis.Client, capacity int, refillPerSecond float64, ttl time.Duration) *TokenBucket {
	return &TokenBucket{
		client:   client,
		capacity: capacity,
		refill:   refillPerSecond,
		ttl:      ttl,
	}
}

// Allow consumes a single token for the given key if available.
// Returns allowed flag and current token count.
func (b *TokenBucket) Allow(ctx context.Context, key string) (bool, float64, error) {
	now := time.Now().UnixMilli()
	res, err := bucketScript.Run(ctx, b.client, []string{key}, b.capacity, b.refill, now, b.ttl.Milliseconds()).Result()
	if err != nil {
		return false, 0, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 2 {
		return false, 0, err
	}
	allowed := arr[0].(int64) == 1
	var tokens float64
	switch v := arr[1].(type) {
	case int64:
		tokens = float64(v)
	case float64:
		tokens = v
	default:
		tokens = 0
	}
	return allowed, tokens, nil
}

var bucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2]) -- tokens per second
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'last_ms')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then tokens = capacity end
if last == nil then last = now end

local delta = math.max(0, now - last)
local add = delta / 1000 * refill
tokens = math.min(capacity, tokens + add)

local allowed = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_ms', now)
if ttl > 0 then redis.call('PEXPIRE', key, ttl) end
return {allowed, tokens}
`)
