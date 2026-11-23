package models

import "time"

// ChatMessage represents a single message in a conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

// ChatCompletionRequest represents an OpenAI-compatible chat completion request.
type ChatCompletionRequest struct {
	Model            string        `json:"model"`
	Messages         []ChatMessage `json:"messages"`
	Temperature      float64       `json:"temperature,omitempty"`
	TopP             float64       `json:"top_p,omitempty"`
	N                int           `json:"n,omitempty"`
	Stream           bool          `json:"stream,omitempty"`
	Stop             []string      `json:"stop,omitempty"`
	MaxTokens        int           `json:"max_tokens,omitempty"`
	PresencePenalty  float64       `json:"presence_penalty,omitempty"`
	FrequencyPenalty float64       `json:"frequency_penalty,omitempty"`
	User             string        `json:"user,omitempty"`
	CacheBypass      bool          `json:"cache_bypass,omitempty"`
}

// ChatChoice represents a single choice in the completion response.
type ChatChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

// Usage represents token usage statistics.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// LlamaTimings represents llama.cpp execution performance metrics.
type LlamaTimings struct {
	PromptN         int     `json:"prompt_n,omitempty"`
	PromptMS        float64 `json:"prompt_ms,omitempty"`
	PredictedN      int     `json:"predicted_n,omitempty"`
	PredictedMS     float64 `json:"predicted_ms,omitempty"`
	PromptPerSecond float64 `json:"prompt_per_second,omitempty"`
	PredictedPerSec float64 `json:"predicted_per_second,omitempty"`
}

// ChatCompletionResponse represents an OpenAI-compatible completion response.
type ChatCompletionResponse struct {
	ID                 string        `json:"id"`
	Object             string        `json:"object"`
	Created            int64         `json:"created"`
	Model              string        `json:"model"`
	Choices            []ChatChoice  `json:"choices"`
	Usage              Usage         `json:"usage"`
	Cached             bool          `json:"cached,omitempty"`
	LatencyMs          int64         `json:"latency_ms,omitempty"`
	TimeToFirstTokenMs int64         `json:"ttft_ms,omitempty"`
	Timings            *LlamaTimings `json:"timings,omitempty"`
}

// ChatCompletionStreamChoice represents a streaming delta choice.
type ChatCompletionStreamChoice struct {
	Index        int         `json:"index"`
	Delta        ChatMessage `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

// ChatCompletionStreamResponse represents an SSE chunk for streaming responses.
type ChatCompletionStreamResponse struct {
	ID      string                       `json:"id"`
	Object  string                       `json:"object"`
	Created int64                        `json:"created"`
	Model   string                       `json:"model"`
	Choices []ChatCompletionStreamChoice `json:"choices"`
}

// ModelMeta holds llama.cpp/GGUF specific model metadata.
type ModelMeta struct {
	FType string `json:"ftype,omitempty"`
	NCtx  int    `json:"n_ctx,omitempty"`
}

// ModelInfo represents available model metadata.
type ModelInfo struct {
	ID            string     `json:"id"`
	Object        string     `json:"object"`
	Created       int64      `json:"created"`
	OwnedBy       string     `json:"owned_by"`
	Quantization  string     `json:"quantization,omitempty"` // e.g. "AWQ-4bit", "GGUF-Q4_K_M"
	ContextLength int        `json:"context_length,omitempty"`
	Meta          *ModelMeta `json:"meta,omitempty"`
}

// ModelListResponse represents the list of supported models.
type ModelListResponse struct {
	Object string      `json:"object"`
	Data   []ModelInfo `json:"data"`
}

// RequestMetadata stores audit and latency information for PostgreSQL logging.
type RequestMetadata struct {
	RequestID        string        `json:"request_id"`
	Model            string        `json:"model"`
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Latency          time.Duration `json:"latency"`
	TTFT             time.Duration `json:"ttft"`
	Cached           bool          `json:"cached"`
	ClientIP         string        `json:"client_ip"`
	StatusCode       int           `json:"status_code"`
	ErrorMessage     string        `json:"error_message,omitempty"`
	CreatedAt        time.Time     `json:"created_at"`
}
