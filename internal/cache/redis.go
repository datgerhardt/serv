package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"serv/internal/metrics"
	"serv/internal/models"
)

// Cache defines the interface for LLM response caching.
type Cache interface {
	Get(ctx context.Context, req *models.ChatCompletionRequest) (*models.ChatCompletionResponse, bool, error)
	Set(ctx context.Context, req *models.ChatCompletionRequest, resp *models.ChatCompletionResponse) error
	Close() error
}

// RedisCache implements Cache backed by Redis.
type RedisCache struct {
	client  *redis.Client
	ttl     time.Duration
	enabled bool
}

// NewRedisCache initializes a connection to Redis with validation.
func NewRedisCache(addr, password string, db int, ttl time.Duration, enabled bool) (*RedisCache, error) {
	if !enabled {
		log.Println("Redis caching is disabled via configuration")
		return &RedisCache{enabled: false}, nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Could not connect to Redis at %s: %v (Cache fallback to bypass)", addr, err)
		return &RedisCache{client: rdb, ttl: ttl, enabled: false}, nil
	}

	log.Printf("Successfully connected to Redis at %s (TTL: %v)", addr, ttl)
	return &RedisCache{
		client:  rdb,
		ttl:     ttl,
		enabled: true,
	}, nil
}

// GenerateKey computes a canonical deterministic SHA-256 hash representing the query parameters.
func (c *RedisCache) GenerateKey(req *models.ChatCompletionRequest) string {
	var sb strings.Builder
	sb.WriteString(req.Model)
	sb.WriteString(";")
	sb.WriteString(fmt.Sprintf("t=%.3f;p=%.3f;m=%d;", req.Temperature, req.TopP, req.MaxTokens))

	for _, msg := range req.Messages {
		sb.WriteString(msg.Role)
		sb.WriteString(":")
		sb.WriteString(strings.TrimSpace(msg.Content))
		sb.WriteString("|")
	}

	hasher := sha256.New()
	hasher.Write([]byte(sb.String()))
	return "llm:cache:" + hex.EncodeToString(hasher.Sum(nil))
}

// Get fetches a cached completion response.
func (c *RedisCache) Get(ctx context.Context, req *models.ChatCompletionRequest) (*models.ChatCompletionResponse, bool, error) {
	if !c.enabled || c.client == nil || req.CacheBypass || req.Stream {
		return nil, false, nil
	}

	start := time.Now()
	key := c.GenerateKey(req)

	val, err := c.client.Get(ctx, key).Result()
	duration := time.Since(start).Seconds()
	metrics.CacheLatency.Observe(duration)

	if err == redis.Nil {
		metrics.CacheMissesTotal.Inc()
		return nil, false, nil
	} else if err != nil {
		// Log redis error without failing user request
		log.Printf("Redis GET error: %v", err)
		metrics.CacheMissesTotal.Inc()
		return nil, false, err
	}

	var cachedResp models.ChatCompletionResponse
	if err := json.Unmarshal([]byte(val), &cachedResp); err != nil {
		log.Printf("JSON unmarshal error for key %s: %v", key, err)
		return nil, false, err
	}

	cachedResp.Cached = true
	metrics.CacheHitsTotal.Inc()
	return &cachedResp, true, nil
}

// Set stores a generated completion response into Redis.
func (c *RedisCache) Set(ctx context.Context, req *models.ChatCompletionRequest, resp *models.ChatCompletionResponse) error {
	if !c.enabled || c.client == nil || req.CacheBypass || req.Stream {
		return nil
	}

	start := time.Now()
	key := c.GenerateKey(req)

	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal cache response: %w", err)
	}

	err = c.client.Set(ctx, key, data, c.ttl).Err()
	duration := time.Since(start).Seconds()
	metrics.CacheLatency.Observe(duration)

	if err != nil {
		log.Printf("Redis SET error for key %s: %v", key, err)
		return err
	}

	return nil
}

// Close closes the Redis connection pool.
func (c *RedisCache) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Client returns the underlying Redis client for shared usage (e.g., rate limiting).
func (c *RedisCache) Client() *redis.Client {
	if c == nil || !c.enabled {
		return nil
	}
	return c.client
}
