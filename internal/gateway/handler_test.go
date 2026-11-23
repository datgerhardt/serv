package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"serv/internal/backend"
	"serv/internal/batcher"
	"serv/internal/models"
	"serv/internal/ratelimit"
	"serv/internal/storage"
)

// mockCache implements cache.Cache for testing.
type mockCache struct {
	cachedResp *models.ChatCompletionResponse
	hit        bool
}

func (m *mockCache) Get(ctx context.Context, req *models.ChatCompletionRequest) (*models.ChatCompletionResponse, bool, error) {
	return m.cachedResp, m.hit, nil
}
func (m *mockCache) Set(ctx context.Context, req *models.ChatCompletionRequest, resp *models.ChatCompletionResponse) error {
	return nil
}
func (m *mockCache) Close() error { return nil }

func TestHandler_Healthz(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, "model-test", "AWQ-4bit")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	h.HandleHealthz(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}

	expected := `{"status":"healthy"}`
	if rr.Body.String() != expected {
		t.Errorf("Expected body %s, got %s", expected, rr.Body.String())
	}
}

func TestHandler_Models(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil, "meta-llama/Llama-3-8B-Instruct-AWQ", "AWQ-4bit")

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rr := httptest.NewRecorder()

	h.HandleModels(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}
}

func TestHandler_CacheHit(t *testing.T) {
	mockC := &mockCache{
		hit: true,
		cachedResp: &models.ChatCompletionResponse{
			ID:    "cached-id",
			Model: "model-test",
			Choices: []models.ChatChoice{
				{
					Message: models.ChatMessage{Role: "assistant", Content: "Cached response"},
				},
			},
			Usage: models.Usage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
		},
	}

	audit := storage.NewAsyncAuditWriter(nil, 10, 5, time.Second)
	defer audit.Stop()

	client := backend.NewClient("http://dummy", time.Second)
	b := batcher.NewDynamicBatcher(client, 2, time.Second, 10, 1, "model-test")
	defer b.Stop()

	h := NewHandler(b, mockC, audit, client, "model-test", "AWQ-4bit")

	reqBody := `{"model":"model-test","messages":[{"role":"user","content":"Hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	rr := httptest.NewRecorder()

	h.HandleChatCompletions(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d. Body: %s", rr.Code, rr.Body.String())
	}

	if cacheHeader := rr.Header().Get("X-Cache-Status"); cacheHeader != "HIT" {
		t.Errorf("Expected X-Cache-Status HIT, got %s", cacheHeader)
	}
}

func TestServer_RateLimiting(t *testing.T) {
	mockC := &mockCache{
		hit: true,
		cachedResp: &models.ChatCompletionResponse{
			ID:    "cached-id",
			Model: "model-test",
			Choices: []models.ChatChoice{
				{Message: models.ChatMessage{Role: "assistant", Content: "Ok"}},
			},
		},
	}

	audit := storage.NewAsyncAuditWriter(nil, 10, 5, time.Second)
	defer audit.Stop()

	client := backend.NewClient("http://dummy", time.Second)
	b := batcher.NewDynamicBatcher(client, 2, time.Second, 10, 1, "model-test")
	defer b.Stop()

	h := NewHandler(b, mockC, audit, client, "model-test", "AWQ-4bit")

	// Create memory limiter with limit of 2 requests
	limiter := ratelimit.NewMemoryLimiter(2)
	defer limiter.Close()

	srv := NewServer("0", h, limiter, audit)

	// 1st request -> Allowed
	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"Hi"}]}`))
	req1.RemoteAddr = "192.168.1.100:1234"
	rr1 := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Fatalf("Expected 1st request status 200, got %d", rr1.Code)
	}
	if rem := rr1.Header().Get("X-RateLimit-Remaining"); rem != "1" {
		t.Errorf("Expected remaining 1, got %s", rem)
	}

	// 2nd request -> Allowed
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"Hi"}]}`))
	req2.RemoteAddr = "192.168.1.100:1234"
	rr2 := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("Expected 2nd request status 200, got %d", rr2.Code)
	}
	if rem := rr2.Header().Get("X-RateLimit-Remaining"); rem != "0" {
		t.Errorf("Expected remaining 0, got %s", rem)
	}

	// 3rd request -> Blocked with 429
	req3 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"Hi"}]}`))
	req3.RemoteAddr = "192.168.1.100:1234"
	rr3 := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rr3, req3)

	if rr3.Code != http.StatusTooManyRequests {
		t.Fatalf("Expected 3rd request status 429 Too Many Requests, got %d", rr3.Code)
	}
	if retryAfter := rr3.Header().Get("Retry-After"); retryAfter == "" {
		t.Errorf("Expected Retry-After header on 429 response")
	}

	// 4th request from a DIFFERENT IP -> Allowed!
	req4 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"Hi"}]}`))
	req4.RemoteAddr = "10.0.0.5:5678"
	rr4 := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rr4, req4)

	if rr4.Code != http.StatusOK {
		t.Fatalf("Expected different IP to be allowed with 200, got %d", rr4.Code)
	}
}

func TestServer_StreamingChatThroughMiddleware(t *testing.T) {
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		for _, tok := range []string{"Hello", " ", "stream"} {
			chunk := models.ChatCompletionStreamResponse{
				ID:    "chunk-1",
				Model: "model-test",
				Choices: []models.ChatCompletionStreamChoice{
					{Index: 0, Delta: models.ChatMessage{Content: tok}},
				},
			}
			b, _ := json.Marshal(chunk)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", string(b))
			flusher.Flush()
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer mockBackend.Close()

	audit := storage.NewAsyncAuditWriter(nil, 10, 5, time.Second)
	defer audit.Stop()

	client := backend.NewClient(mockBackend.URL, 5*time.Second)
	b := batcher.NewDynamicBatcher(client, 2, time.Second, 10, 1, "model-test")
	defer b.Stop()

	h := NewHandler(b, &mockCache{}, audit, client, "model-test", "AWQ-4bit")
	srv := NewServer("0", h, &ratelimit.NoopLimiter{}, audit)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(`{"model":"model-test","messages":[{"role":"user","content":"Hi"}],"stream":true}`),
	)
	rr := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected status 200 for streaming through Server middleware, got %d: %s", rr.Code, rr.Body.String())
	}
	if !rr.Flushed {
		t.Errorf("Expected ResponseRecorder to be flushed during SSE stream")
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Expected Content-Type text/event-stream, got %q", ct)
	}
	if reqID := rr.Header().Get(RequestIDHeader); reqID == "" {
		t.Errorf("Expected generated %s response header", RequestIDHeader)
	} else if req.Header.Get(RequestIDHeader) != reqID {
		t.Errorf("Expected generated %s to be propagated to request headers", RequestIDHeader)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"content":"stream"`) || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("Unexpected SSE stream body: %s", body)
	}
}

func TestHandler_RequestBodyLimit(t *testing.T) {
	h := NewHandler(nil, &mockCache{}, nil, nil, "model-test", "AWQ-4bit")

	oversized := strings.Repeat("a", maxRequestBodyBytes+1024)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(oversized))
	rr := httptest.NewRecorder()

	h.HandleChatCompletions(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("Expected status 413 for oversized body, got %d", rr.Code)
	}
}
