package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"serv/internal/models"
)

func TestClient_BatchFallbackToChatCompletions(t *testing.T) {
	var chatCalls int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/batch/chat/completions" {
			// Simulate standard llama-server / vLLM returning 404 for batch endpoint
			http.NotFound(w, r)
			return
		}

		if r.URL.Path == "/v1/chat/completions" {
			atomic.AddInt32(&chatCalls, 1)
			var req models.ChatCompletionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(models.ChatCompletionResponse{
				ID:      "chatcmpl-test",
				Model:   req.Model,
				Choices: []models.ChatChoice{
					{
						Index: 0,
						Message: models.ChatMessage{
							Role:    "assistant",
							Content: fmt.Sprintf("Response to: %s", req.Messages[0].Content),
						},
						FinishReason: "stop",
					},
				},
				Usage: models.Usage{
					PromptTokens:     10,
					CompletionTokens: 5,
					TotalTokens:      15,
				},
				Timings: &models.LlamaTimings{
					PromptMS:    12.5,
					PredictedMS: 25.0,
				},
			})
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(server.URL, 5*time.Second)

	batchReq := &BatchInferenceRequest{
		Model: "llama-3.2",
		Items: []BatchInferenceItem{
			{
				ID: "item-1",
				Messages: []models.ChatMessage{
					{Role: "user", Content: "Hello 1"},
				},
				MaxTokens: 32,
			},
			{
				ID: "item-2",
				Messages: []models.ChatMessage{
					{Role: "user", Content: "Hello 2"},
				},
				MaxTokens: 32,
			},
		},
	}

	resp, err := client.ExecuteBatch(context.Background(), batchReq)
	if err != nil {
		t.Fatalf("ExecuteBatch failed: %v", err)
	}

	if len(resp.Results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(resp.Results))
	}

	if atomic.LoadInt32(&chatCalls) != 2 {
		t.Errorf("Expected 2 calls to /v1/chat/completions, got %d", atomic.LoadInt32(&chatCalls))
	}

	// Verify that subsequent batch calls skip /v1/batch/chat/completions directly
	_, err = client.ExecuteBatch(context.Background(), batchReq)
	if err != nil {
		t.Fatalf("Second ExecuteBatch failed: %v", err)
	}

	if atomic.LoadInt32(&chatCalls) != 4 {
		t.Errorf("Expected 4 total calls to /v1/chat/completions, got %d", atomic.LoadInt32(&chatCalls))
	}
}

func TestClient_StreamChatCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{"Hello", " ", "world"}
		for _, c := range chunks {
			chunk := models.ChatCompletionStreamResponse{
				ID:    "chunk-id",
				Model: "llama-3.2",
				Choices: []models.ChatCompletionStreamChoice{
					{
						Index: 0,
						Delta: models.ChatMessage{Content: c},
					},
				},
			}
			data, _ := json.Marshal(chunk)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", string(data))
			flusher.Flush()
		}

		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer server.Close()

	client := NewClient(server.URL, 5*time.Second)

	var received []string
	err := client.StreamChatCompletion(context.Background(), &models.ChatCompletionRequest{
		Model: "llama-3.2",
		Messages: []models.ChatMessage{
			{Role: "user", Content: "Hi"},
		},
	}, func(chunk *models.ChatCompletionStreamResponse) error {
		if len(chunk.Choices) > 0 {
			received = append(received, chunk.Choices[0].Delta.Content)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("StreamChatCompletion failed: %v", err)
	}

	if len(received) != 3 {
		t.Fatalf("Expected 3 chunks, got %d", len(received))
	}
}
