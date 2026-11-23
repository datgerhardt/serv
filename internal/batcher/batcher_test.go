package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"serv/internal/backend"
	"serv/internal/models"
)

func TestDynamicBatcher_Coalescing(t *testing.T) {
	var batchCalls int32
	var totalItems int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/batch/chat/completions" {
			http.NotFound(w, r)
			return
		}

		var batchReq backend.BatchInferenceRequest
		if err := json.NewDecoder(r.Body).Decode(&batchReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		atomic.AddInt32(&batchCalls, 1)
		atomic.AddInt32(&totalItems, int32(len(batchReq.Items)))

		results := make([]backend.BatchInferenceItemResponse, len(batchReq.Items))
		for i, item := range batchReq.Items {
			results[i] = backend.BatchInferenceItemResponse{
				ID: item.ID,
				Choice: models.ChatChoice{
					Index: 0,
					Message: models.ChatMessage{
						Role:    "assistant",
						Content: fmt.Sprintf("Response for %s", item.ID),
					},
					FinishReason: "stop",
				},
				Usage: models.Usage{
					PromptTokens:     10,
					CompletionTokens: 5,
					TotalTokens:      15,
				},
				TimeToFirstToken: 25,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.BatchInferenceResponse{
			Model:   batchReq.Model,
			Results: results,
		})
	}))
	defer server.Close()

	client := backend.NewClient(server.URL, 5*time.Second)
	// Batch size 4, flush timeout 50ms
	b := NewDynamicBatcher(client, 4, 50*time.Millisecond, 100, 4, "test-model")
	defer b.Stop()

	// Submit 8 concurrent requests
	numRequests := 8
	var wg sync.WaitGroup
	wg.Add(numRequests)

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			req := &models.ChatCompletionRequest{
				Model: "test-model",
				Messages: []models.ChatMessage{
					{Role: "user", Content: fmt.Sprintf("Prompt %d", idx)},
				},
			}

			resp, err := b.Submit(context.Background(), req)
			if err != nil {
				t.Errorf("Request %d failed: %v", idx, err)
				return
			}

			if resp == nil || len(resp.Choices) == 0 {
				t.Errorf("Request %d returned empty response", idx)
				return
			}
		}(i)
	}

	wg.Wait()

	calls := atomic.LoadInt32(&batchCalls)
	items := atomic.LoadInt32(&totalItems)

	if items != int32(numRequests) {
		t.Errorf("Expected %d total items processed, got %d", numRequests, items)
	}

	// 8 items with batch size 4 should result in approximately 2 batch calls (or at most 3 depending on scheduling)
	if calls > 4 {
		t.Errorf("Dynamic batching suboptimal: 8 requests took %d batch calls", calls)
	}
}

func TestDynamicBatcher_TimeoutFlush(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batchReq backend.BatchInferenceRequest
		_ = json.NewDecoder(r.Body).Decode(&batchReq)

		results := make([]backend.BatchInferenceItemResponse, len(batchReq.Items))
		for i, item := range batchReq.Items {
			results[i] = backend.BatchInferenceItemResponse{
				ID: item.ID,
				Choice: models.ChatChoice{
					Index: 0,
					Message: models.ChatMessage{
						Role:    "assistant",
						Content: "Fast reply",
					},
					FinishReason: "stop",
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.BatchInferenceResponse{
			Model:   batchReq.Model,
			Results: results,
		})
	}))
	defer server.Close()

	client := backend.NewClient(server.URL, 5*time.Second)
	// Max batch size is 10, but we send only 1 item; it should flush after 25ms timeout
	b := NewDynamicBatcher(client, 10, 25*time.Millisecond, 100, 2, "test-model")
	defer b.Stop()

	start := time.Now()
	req := &models.ChatCompletionRequest{
		Model: "test-model",
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Single prompt"},
		},
	}

	resp, err := b.Submit(context.Background(), req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	if resp.Choices[0].Message.Content != "Fast reply" {
		t.Errorf("Unexpected content: %s", resp.Choices[0].Message.Content)
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("Timeout flush took too long: %v", elapsed)
	}
}
