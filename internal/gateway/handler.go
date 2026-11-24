package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"serv/internal/backend"
	"serv/internal/batcher"
	"serv/internal/cache"
	"serv/internal/metrics"
	"serv/internal/models"
	"serv/internal/storage"
)

const maxRequestBodyBytes = 16 << 20 // 16 MiB (matches ingress proxy-body-size)

// Handler houses all HTTP endpoints for the LLM Gateway.
type Handler struct {
	batcher      *batcher.DynamicBatcher
	cache        cache.Cache
	auditWriter  *storage.AsyncAuditWriter
	backend      *backend.Client
	defaultModel string
	quantization string
}

// NewHandler constructs a gateway handler instance.
func NewHandler(
	b *batcher.DynamicBatcher,
	c cache.Cache,
	audit *storage.AsyncAuditWriter,
	backend *backend.Client,
	defaultModel string,
	quantization string,
) *Handler {
	return &Handler{
		batcher:      b,
		cache:        c,
		auditWriter:  audit,
		backend:      backend,
		defaultModel: defaultModel,
		quantization: quantization,
	}
}

// HandleChatCompletions routes inference requests through Cache, Dynamic Batcher, or SSE Streamer.
func (h *Handler) HandleChatCompletions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := r.Header.Get(RequestIDHeader)
	clientIP := r.RemoteAddr

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, `{"error":{"message":"Request body exceeds maximum allowed size"}}`, http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, `{"error":{"message":"Failed to read request body"}}`, http.StatusBadRequest)
		return
	}

	var req models.ChatCompletionRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		http.Error(w, `{"error":{"message":"Invalid JSON payload"}}`, http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, `{"error":{"message":"Messages array cannot be empty"}}`, http.StatusBadRequest)
		return
	}

	if req.Model == "" {
		req.Model = h.defaultModel
	}

	// Cache bypass header override
	if r.Header.Get("X-Cache-Bypass") == "true" {
		req.CacheBypass = true
	}

	// --- 1. STREAMING REQUEST EXECUTION ---
	if req.Stream {
		h.handleStreamingChat(w, r, &req, reqID, clientIP, start)
		return
	}

	// --- 2. EXACT / PROMPT CACHE LOOKUP (Redis) ---
	cachedResp, hit, err := h.cache.Get(r.Context(), &req)
	if err == nil && hit && cachedResp != nil {
		cachedResp.LatencyMs = time.Since(start).Milliseconds()

		// Asynchronously log cached telemetry
		h.auditWriter.Enqueue(models.RequestMetadata{
			RequestID:        reqID,
			Model:            req.Model,
			PromptTokens:     cachedResp.Usage.PromptTokens,
			CompletionTokens: cachedResp.Usage.CompletionTokens,
			TotalTokens:      cachedResp.Usage.TotalTokens,
			Latency:          time.Since(start),
			TTFT:             0,
			Cached:           true,
			ClientIP:         clientIP,
			StatusCode:       http.StatusOK,
			CreatedAt:        time.Now(),
		})

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache-Status", "HIT")
		_ = json.NewEncoder(w).Encode(cachedResp)
		return
	}

	// --- 3. DYNAMIC MICRO-BATCHING INFERENCE ---
	w.Header().Set("X-Cache-Status", "MISS")
	resp, err := h.batcher.Submit(r.Context(), &req)
	if err != nil {
		log.Printf("Batch inference error for %s: %v", reqID, err)
		h.auditWriter.Enqueue(models.RequestMetadata{
			RequestID:    reqID,
			Model:        req.Model,
			Latency:      time.Since(start),
			Cached:       false,
			ClientIP:     clientIP,
			StatusCode:   http.StatusInternalServerError,
			ErrorMessage: err.Error(),
			CreatedAt:    time.Now(),
		})
		http.Error(w, fmt.Sprintf(`{"error":{"message":%q,"type":"inference_error"}}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Cache successful response in Redis using detached context with timeout
	go func(reqCopy models.ChatCompletionRequest, respCopy models.ChatCompletionResponse) {
		cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.cache.Set(cacheCtx, &reqCopy, &respCopy)
	}(req, *resp)

	// Update Prometheus metrics
	metrics.PromptTokensTotal.WithLabelValues(req.Model).Add(float64(resp.Usage.PromptTokens))
	metrics.CompletionTokensTotal.WithLabelValues(req.Model).Add(float64(resp.Usage.CompletionTokens))

	// Record audit metadata
	h.auditWriter.Enqueue(models.RequestMetadata{
		RequestID:        reqID,
		Model:            req.Model,
		PromptTokens:     resp.Usage.PromptTokens,
		CompletionTokens: resp.Usage.CompletionTokens,
		TotalTokens:      resp.Usage.TotalTokens,
		Latency:          time.Since(start),
		TTFT:             time.Duration(resp.TimeToFirstTokenMs) * time.Millisecond,
		Cached:           false,
		ClientIP:         clientIP,
		StatusCode:       http.StatusOK,
		CreatedAt:        time.Now(),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handleStreamingChat processes Server-Sent Events (SSE) token streaming.
func (h *Handler) handleStreamingChat(
	w http.ResponseWriter,
	r *http.Request,
	req *models.ChatCompletionRequest,
	reqID string,
	clientIP string,
	start time.Time,
) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":{"message":"Streaming unsupported by connection"}}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	var firstTokenOnce bool
	var ttft time.Duration
	completionTokens := 0

	err := h.backend.StreamChatCompletion(r.Context(), req, func(chunk *models.ChatCompletionStreamResponse) error {
		if !firstTokenOnce {
			ttft = time.Since(start)
			firstTokenOnce = true
			metrics.TimeToFirstToken.WithLabelValues(req.Model).Observe(ttft.Seconds())
		}

		completionTokens++
		chunkBytes, err := json.Marshal(chunk)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "data: %s\n\n", string(chunkBytes))
		flusher.Flush()
		return err
	})

	if err != nil {
		log.Printf("Streaming error for %s: %v", reqID, err)
		h.auditWriter.Enqueue(models.RequestMetadata{
			RequestID:    reqID,
			Model:        req.Model,
			Latency:      time.Since(start),
			TTFT:         ttft,
			Cached:       false,
			ClientIP:     clientIP,
			StatusCode:   http.StatusInternalServerError,
			ErrorMessage: err.Error(),
			CreatedAt:    time.Now(),
		})
		return
	}

	// Write [DONE] sentinel
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()

	// Update token and audit metrics
	metrics.CompletionTokensTotal.WithLabelValues(req.Model).Add(float64(completionTokens))

	h.auditWriter.Enqueue(models.RequestMetadata{
		RequestID:        reqID,
		Model:            req.Model,
		CompletionTokens: completionTokens,
		TotalTokens:      completionTokens,
		Latency:          time.Since(start),
		TTFT:             ttft,
		Cached:           false,
		ClientIP:         clientIP,
		StatusCode:       http.StatusOK,
		CreatedAt:        time.Now(),
	})
}

// HandleModels returns metadata for served quantized LLMs.
func (h *Handler) HandleModels(w http.ResponseWriter, r *http.Request) {
	if h.backend != nil {
		if list, err := h.backend.GetModels(r.Context()); err == nil && len(list.Data) > 0 {
			for i := range list.Data {
				if list.Data[i].Meta != nil {
					if list.Data[i].Quantization == "" && list.Data[i].Meta.FType != "" {
						list.Data[i].Quantization = "GGUF-" + list.Data[i].Meta.FType
					}
					if list.Data[i].ContextLength == 0 && list.Data[i].Meta.NCtx > 0 {
						list.Data[i].ContextLength = list.Data[i].Meta.NCtx
					}
				}
				if list.Data[i].Quantization == "" {
					list.Data[i].Quantization = h.quantization
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(list)
			return
		}
	}

	resp := models.ModelListResponse{
		Object: "list",
		Data: []models.ModelInfo{
			{
				ID:            h.defaultModel,
				Object:        "model",
				Created:       time.Now().Unix(),
				OwnedBy:       "quantized-serving-system",
				Quantization:  h.quantization,
				ContextLength: 8192,
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleHealthz responds for Kubernetes liveness probe.
func (h *Handler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy"}`))
}

// HandleReadyz responds for Kubernetes readiness probe.
func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}
