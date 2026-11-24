package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"serv/internal/models"
)

// Client communicates with the quantized LLM backend (vLLM or llama.cpp).
type Client struct {
	baseURL         string
	httpClient      *http.Client
	hasBatchSupport int32 // 0: unknown, 1: supported, 2: not supported (fallback to /v1/chat/completions)
}

// NewClient creates a new backend HTTP client with optimized connection pooling.
func NewClient(baseURL string, timeout time.Duration) *Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // streaming SSE latency optimization
	}

	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
	}
}

// ExecuteBatch sends a batched inference request to the quantized engine.
// If the backend doesn't support /v1/batch/chat/completions (e.g. standard llama-server or vLLM),
// it automatically falls back to parallel execution against standard /v1/chat/completions.
func (c *Client) ExecuteBatch(ctx context.Context, batchReq *BatchInferenceRequest) (*BatchInferenceResponse, error) {
	// If already identified as lacking dedicated batch endpoint, execute directly via chat completions
	if atomic.LoadInt32(&c.hasBatchSupport) == 2 {
		return c.executeBatchViaChatCompletions(ctx, batchReq)
	}

	endpoint := fmt.Sprintf("%s/v1/batch/chat/completions", c.baseURL)

	payload, err := json.Marshal(batchReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch request to backend: %w", err)
	}
	defer resp.Body.Close()

	// If 404 Not Found or 405 Method Not Allowed, fallback to standard OpenAI /v1/chat/completions
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		atomic.StoreInt32(&c.hasBatchSupport, 2)
		return c.executeBatchViaChatCompletions(ctx, batchReq)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("backend returned HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var batchResp BatchInferenceResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("failed to decode batch response: %w", err)
	}

	atomic.StoreInt32(&c.hasBatchSupport, 1)
	return &batchResp, nil
}

// executeBatchViaChatCompletions executes batch items concurrently via /v1/chat/completions.
func (c *Client) executeBatchViaChatCompletions(ctx context.Context, batchReq *BatchInferenceRequest) (*BatchInferenceResponse, error) {
	results := make([]BatchInferenceItemResponse, len(batchReq.Items))
	var wg sync.WaitGroup

	for i, item := range batchReq.Items {
		wg.Add(1)
		go func(idx int, it BatchInferenceItem) {
			defer wg.Done()

			itemReq := &models.ChatCompletionRequest{
				Model:       batchReq.Model,
				Messages:    it.Messages,
				MaxTokens:   it.MaxTokens,
				Temperature: batchReq.Temperature,
				TopP:        batchReq.TopP,
				Stream:      false,
			}

			start := time.Now()
			chatResp, err := c.ExecuteChatCompletion(ctx, itemReq)
			elapsed := time.Since(start).Milliseconds()

			if err != nil {
				results[idx] = BatchInferenceItemResponse{
					ID:    it.ID,
					Error: err.Error(),
				}
				return
			}

			choice := models.ChatChoice{
				Index:        0,
				FinishReason: "stop",
			}
			if len(chatResp.Choices) > 0 {
				choice = chatResp.Choices[0]
			}

			ttft := elapsed
			if chatResp.Timings != nil && chatResp.Timings.PromptMS > 0 {
				ttft = int64(chatResp.Timings.PromptMS)
			}

			results[idx] = BatchInferenceItemResponse{
				ID:               it.ID,
				Choice:           choice,
				Usage:            chatResp.Usage,
				TimeToFirstToken: ttft,
			}
		}(i, item)
	}

	wg.Wait()

	return &BatchInferenceResponse{
		Model:   batchReq.Model,
		Results: results,
	}, nil
}

// ExecuteChatCompletion sends a single non-streaming chat completion request.
func (c *Client) ExecuteChatCompletion(ctx context.Context, req *models.ChatCompletionRequest) (*models.ChatCompletionResponse, error) {
	endpoint := fmt.Sprintf("%s/v1/chat/completions", c.baseURL)

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal chat request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request to backend: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("backend returned HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp models.ChatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, fmt.Errorf("failed to decode completion response: %w", err)
	}

	return &chatResp, nil
}

// StreamChatCompletion forwards a streaming request to backend and emits SSE chunks to the callback.
func (c *Client) StreamChatCompletion(ctx context.Context, req *models.ChatCompletionRequest, chunkCallback func(chunk *models.ChatCompletionStreamResponse) error) error {
	endpoint := fmt.Sprintf("%s/v1/chat/completions", c.baseURL)

	reqCopy := *req
	reqCopy.Stream = true

	payload, err := json.Marshal(reqCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to dispatch streaming request to backend: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backend returned HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			if data == "[DONE]" {
				break
			}

			var chunk models.ChatCompletionStreamResponse
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue
			}

			if err := chunkCallback(&chunk); err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}

// GetModels queries available quantized models from the worker backend.
func (c *Client) GetModels(ctx context.Context) (*models.ModelListResponse, error) {
	endpoint := fmt.Sprintf("%s/v1/models", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend returned status %d", resp.StatusCode)
	}

	var list models.ModelListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}

	return &list, nil
}
