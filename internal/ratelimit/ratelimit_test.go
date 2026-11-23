package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryLimiter_Enforcement(t *testing.T) {
	limit := 3
	limiter := NewMemoryLimiter(limit)
	defer limiter.Close()

	ctx := context.Background()
	clientKey := "client-123"

	// First 3 requests should be allowed
	for i := 0; i < limit; i++ {
		res, err := limiter.Allow(ctx, clientKey)
		if err != nil {
			t.Fatalf("Unexpected error on request %d: %v", i+1, err)
		}
		if !res.Allowed {
			t.Fatalf("Expected request %d to be allowed", i+1)
		}
		expectedRemaining := limit - (i + 1)
		if res.Remaining != expectedRemaining {
			t.Errorf("Expected remaining %d, got %d", expectedRemaining, res.Remaining)
		}
	}

	// 4th request must be rejected
	res, err := limiter.Allow(ctx, clientKey)
	if err != nil {
		t.Fatalf("Unexpected error on 4th request: %v", err)
	}
	if res.Allowed {
		t.Errorf("Expected 4th request to be blocked by rate limiter")
	}
	if res.Remaining != 0 {
		t.Errorf("Expected remaining 0, got %d", res.Remaining)
	}

	// A different client key should still be allowed
	otherClientRes, err := limiter.Allow(ctx, "client-456")
	if err != nil || !otherClientRes.Allowed {
		t.Errorf("Different client key should be allowed independently")
	}
}

func TestMemoryLimiter_WindowReset(t *testing.T) {
	// Limiter with short custom window behavior
	limiter := &MemoryLimiter{
		limits: make(map[string]*clientBucket),
		limit:  1,
		window: 50 * time.Millisecond,
	}

	ctx := context.Background()
	key := "test-reset"

	res1, _ := limiter.Allow(ctx, key)
	if !res1.Allowed {
		t.Fatalf("First request should be allowed")
	}

	res2, _ := limiter.Allow(ctx, key)
	if res2.Allowed {
		t.Fatalf("Immediate second request should be blocked")
	}

	// Wait for window to expire
	time.Sleep(60 * time.Millisecond)

	res3, _ := limiter.Allow(ctx, key)
	if !res3.Allowed {
		t.Errorf("Request after window expiration should be allowed")
	}
}

func TestNoopLimiter(t *testing.T) {
	noop := &NoopLimiter{}
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		res, err := noop.Allow(ctx, "any-key")
		if err != nil || !res.Allowed {
			t.Fatalf("Noop limiter should never block requests")
		}
	}
}
