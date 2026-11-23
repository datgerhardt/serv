package cache

import (
	"context"
	"testing"
	"time"

	"serv/internal/models"
)

func TestGenerateKey(t *testing.T) {
	c := &RedisCache{enabled: true}

	req1 := &models.ChatCompletionRequest{
		Model:       "llama-3-8b",
		Temperature: 0.7,
		TopP:        0.9,
		MaxTokens:   256,
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Hello world"},
		},
	}

	req2 := &models.ChatCompletionRequest{
		Model:       "llama-3-8b",
		Temperature: 0.7,
		TopP:        0.9,
		MaxTokens:   256,
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Hello world"},
		},
	}

	req3 := &models.ChatCompletionRequest{
		Model:       "llama-3-8b",
		Temperature: 0.8, // different temperature
		TopP:        0.9,
		MaxTokens:   256,
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Hello world"},
		},
	}

	key1 := c.GenerateKey(req1)
	key2 := c.GenerateKey(req2)
	key3 := c.GenerateKey(req3)

	if key1 != key2 {
		t.Errorf("Expected key1 == key2, got %s != %s", key1, key2)
	}

	if key1 == key3 {
		t.Errorf("Expected key1 != key3 for different temperatures, got identical: %s", key1)
	}
}

func TestDisabledCache(t *testing.T) {
	c, err := NewRedisCache("localhost:6379", "", 0, time.Minute, false)
	if err != nil {
		t.Fatalf("Unexpected error with disabled cache: %v", err)
	}

	req := &models.ChatCompletionRequest{
		Model: "llama-3-8b",
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Test disabled"},
		},
	}

	resp, hit, err := c.Get(context.Background(), req)
	if err != nil || hit || resp != nil {
		t.Errorf("Expected cache miss with no error on disabled cache, got hit=%v, err=%v", hit, err)
	}

	if err := c.Set(context.Background(), req, &models.ChatCompletionResponse{}); err != nil {
		t.Errorf("Expected nil error on disabled cache Set, got %v", err)
	}
}
