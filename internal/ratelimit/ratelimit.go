package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Result holds the rate limiting decision and metadata.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	ResetAfter time.Duration
}

// Limiter defines the rate limiter interface.
type Limiter interface {
	Allow(ctx context.Context, key string) (*Result, error)
	Close() error
}

// slidingWindowScript is an atomic Redis Lua script implementing sliding window rate limiting.
var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

local clearBefore = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', clearBefore)
local currentCount = redis.call('ZCARD', key)

if currentCount < limit then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, math.ceil(window / 1000000000) + 2)
    return { 1, limit - currentCount - 1, 0 }
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local resetAfterMs = 1000
    if #oldest > 1 then
        local oldestScore = tonumber(oldest[2])
        resetAfterMs = math.max(1, math.ceil((oldestScore + window - now) / 1000000))
    end
    return { 0, 0, resetAfterMs }
end
`)

// RedisLimiter implements distributed sliding window rate limiting using Redis.
type RedisLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
}

// NewRedisLimiter creates a Redis-backed distributed rate limiter.
func NewRedisLimiter(client *redis.Client, rpm int) *RedisLimiter {
	return &RedisLimiter{
		client: client,
		limit:  rpm,
		window: time.Minute,
	}
}

// Allow evaluates the rate limit for a client key in Redis.
func (r *RedisLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	redisKey := fmt.Sprintf("llm:ratelimit:%s", key)
	nowNano := time.Now().UnixNano()
	windowNano := r.window.Nanoseconds()
	member := uuid.New().String()

	res, err := slidingWindowScript.Run(ctx, r.client, []string{redisKey}, nowNano, windowNano, r.limit, member).Result()
	if err != nil {
		return nil, fmt.Errorf("redis rate limit script error: %w", err)
	}

	vals, ok := res.([]interface{})
	if !ok || len(vals) < 3 {
		return nil, fmt.Errorf("unexpected redis script return format: %v", res)
	}

	allowed := vals[0].(int64) == 1
	remaining := int(vals[1].(int64))
	resetAfterMs := vals[2].(int64)

	return &Result{
		Allowed:    allowed,
		Limit:      r.limit,
		Remaining:  remaining,
		ResetAfter: time.Duration(resetAfterMs) * time.Millisecond,
	}, nil
}

func (r *RedisLimiter) Close() error {
	return nil
}

// MemoryLimiter provides an in-memory token bucket / sliding window fallback.
type MemoryLimiter struct {
	mu        sync.Mutex
	limits    map[string]*clientBucket
	limit     int
	window    time.Duration
	stopClean chan struct{}
}

type clientBucket struct {
	timestamps []time.Time
	lastSeen   time.Time
}

// NewMemoryLimiter creates a thread-safe in-memory rate limiter with periodic cleanup.
func NewMemoryLimiter(rpm int) *MemoryLimiter {
	m := &MemoryLimiter{
		limits:    make(map[string]*clientBucket),
		limit:     rpm,
		window:    time.Minute,
		stopClean: make(chan struct{}),
	}

	// Periodic garbage collection for inactive client keys
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.mu.Lock()
				cutoff := time.Now().Add(-5 * time.Minute)
				for k, v := range m.limits {
					if v.lastSeen.Before(cutoff) {
						delete(m.limits, k)
					}
				}
				m.mu.Unlock()
			case <-m.stopClean:
				return
			}
		}
	}()

	return m
}

// Allow evaluates the rate limit in memory.
func (m *MemoryLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-m.window)

	b, exists := m.limits[key]
	if !exists {
		b = &clientBucket{
			timestamps: make([]time.Time, 0, m.limit),
			lastSeen:   now,
		}
		m.limits[key] = b
	}

	b.lastSeen = now

	// Prune timestamps older than window
	valid := b.timestamps[:0]
	for _, t := range b.timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	b.timestamps = valid

	if len(b.timestamps) >= m.limit {
		oldest := b.timestamps[0]
		resetAfter := oldest.Add(m.window).Sub(now)
		if resetAfter < time.Second {
			resetAfter = time.Second
		}

		return &Result{
			Allowed:    false,
			Limit:      m.limit,
			Remaining:  0,
			ResetAfter: resetAfter,
		}, nil
	}

	b.timestamps = append(b.timestamps, now)
	remaining := m.limit - len(b.timestamps)

	return &Result{
		Allowed:    true,
		Limit:      m.limit,
		Remaining:  remaining,
		ResetAfter: m.window,
	}, nil
}

func (m *MemoryLimiter) Close() error {
	close(m.stopClean)
	return nil
}

// NoopLimiter allows all requests when rate limiting is disabled.
type NoopLimiter struct{}

func (n *NoopLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	return &Result{
		Allowed:    true,
		Limit:      math.MaxInt32,
		Remaining:  math.MaxInt32,
		ResetAfter: 0,
	}, nil
}

func (n *NoopLimiter) Close() error {
	return nil
}

// NewLimiter builds the appropriate rate limiter implementation based on configuration.
func NewLimiter(redisClient *redis.Client, rpm int, enabled bool) Limiter {
	if !enabled || rpm <= 0 {
		return &NoopLimiter{}
	}
	if redisClient != nil {
		return NewRedisLimiter(redisClient, rpm)
	}
	return NewMemoryLimiter(rpm)
}
