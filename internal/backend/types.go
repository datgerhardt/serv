package backend

import (
	"serv/internal/models"
)

// BatchInferenceItem represents one request inside a batch.
type BatchInferenceItem struct {
	ID        string               `json:"id"`
	Messages  []models.ChatMessage `json:"messages"`
	MaxTokens int                  `json:"max_tokens"`
}

// BatchInferenceRequest is submitted to the quantized worker backend.
type BatchInferenceRequest struct {
	Model       string               `json:"model"`
	Items       []BatchInferenceItem `json:"items"`
	Temperature float64              `json:"temperature"`
	TopP        float64              `json:"top_p"`
}

// BatchInferenceItemResponse is the individual completion within a batch response.
type BatchInferenceItemResponse struct {
	ID               string            `json:"id"`
	Choice           models.ChatChoice `json:"choice"`
	Usage            models.Usage      `json:"usage"`
	TimeToFirstToken int64             `json:"ttft_ms"`
	Error            string            `json:"error,omitempty"`
}

// BatchInferenceResponse returned by the quantized worker.
type BatchInferenceResponse struct {
	Model   string                       `json:"model"`
	Results []BatchInferenceItemResponse `json:"results"`
}
