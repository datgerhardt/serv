package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"serv/internal/backend"
	"serv/internal/models"
)

var sampleResponses = []string{
	"Quantized LLMs achieve significant memory bandwidth savings by compressing weight matrices to 4-bit integers.",
	"Using dynamic continuous batching reduces queue latency and maximizes matrix multiplication utilization on tensor cores.",
	"AWQ (Activation-aware Weight Quantization) protects critical salient weights while quantizing non-critical channels.",
	"GGUF format enables efficient CPU and GPU hybrid offloading for edge and cloud quantized inference.",
	"Horizontal Pod Autoscaling paired with KEDA monitors active batch queues to scale worker pods dynamically under load.",
}

func main() {
	port := os.Getenv("WORKER_PORT")
	if port == "" {
		port = "8000"
	}

	modelName := os.Getenv("MODEL_NAME")
	if modelName == "" {
		modelName = "meta-llama/Llama-3-8B-Instruct-AWQ"
	}

	quantType := os.Getenv("QUANTIZATION_TYPE")
	if quantType == "" {
		quantType = "AWQ-4bit"
	}

	mux := http.NewServeMux()

	// 1. Batched completions endpoint
	mux.HandleFunc("/v1/batch/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var batchReq backend.BatchInferenceRequest
		if err := json.NewDecoder(r.Body).Decode(&batchReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Simulate quantized kernel execution latency:
		// Base latency ~15ms + 5ms per batched item (AWQ 4-bit acceleration)
		simulatedLatency := 15*time.Millisecond + time.Duration(len(batchReq.Items)*5)*time.Millisecond
		time.Sleep(simulatedLatency)

		results := make([]backend.BatchInferenceItemResponse, len(batchReq.Items))
		for i, item := range batchReq.Items {
			respText := sampleResponses[rand.Intn(len(sampleResponses))]
			promptTokens := 0
			for _, m := range item.Messages {
				promptTokens += len(strings.Fields(m.Content))
			}
			completionTokens := len(strings.Fields(respText))

			results[i] = backend.BatchInferenceItemResponse{
				ID: item.ID,
				Choice: models.ChatChoice{
					Index: 0,
					Message: models.ChatMessage{
						Role:    "assistant",
						Content: respText,
					},
					FinishReason: "stop",
				},
				Usage: models.Usage{
					PromptTokens:     promptTokens,
					CompletionTokens: completionTokens,
					TotalTokens:      promptTokens + completionTokens,
				},
				TimeToFirstToken: 20 + int64(rand.Intn(15)),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(backend.BatchInferenceResponse{
			Model:   batchReq.Model,
			Results: results,
		})
	})

	// 2. Single completions / SSE streaming endpoint
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req models.ChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Stream {
			flusher, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")

			respText := sampleResponses[rand.Intn(len(sampleResponses))]
			words := strings.Fields(respText)
			streamID := "chatcmpl-" + uuid.New().String()

			// Initial TTFT delay
			time.Sleep(30 * time.Millisecond)

			for i, word := range words {
				token := word + " "
				chunk := models.ChatCompletionStreamResponse{
					ID:      streamID,
					Object:  "chat.completion.chunk",
					Created: time.Now().Unix(),
					Model:   req.Model,
					Choices: []models.ChatCompletionStreamChoice{
						{
							Index: 0,
							Delta: models.ChatMessage{
								Content: token,
							},
						},
					},
				}

				if i == len(words)-1 {
					reason := "stop"
					chunk.Choices[0].FinishReason = &reason
				}

				data, _ := json.Marshal(chunk)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()

				// Simulated inter-token latency ~15ms (approx 66 tokens/sec)
				time.Sleep(15 * time.Millisecond)
			}

			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			flusher.Flush()
			return
		}

		// Non-streaming single completion fallback
		time.Sleep(35 * time.Millisecond)
		respText := sampleResponses[rand.Intn(len(sampleResponses))]
		promptTokens := 15
		completionTokens := len(strings.Fields(respText))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(models.ChatCompletionResponse{
			ID:      "chatcmpl-" + uuid.New().String(),
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			Model:   req.Model,
			Choices: []models.ChatChoice{
				{
					Index: 0,
					Message: models.ChatMessage{
						Role:    "assistant",
						Content: respText,
					},
					FinishReason: "stop",
				},
			},
			Usage: models.Usage{
				PromptTokens:     promptTokens,
				CompletionTokens: completionTokens,
				TotalTokens:      promptTokens + completionTokens,
			},
		})
	})

	// 3. Models endpoint
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(models.ModelListResponse{
			Object: "list",
			Data: []models.ModelInfo{
				{
					ID:            modelName,
					Object:        "model",
					Created:       time.Now().Unix(),
					OwnedBy:       "quantized-worker",
					Quantization:  quantType,
					ContextLength: 8192,
				},
			},
		})
	})

	// Health probes
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})

	log.Printf("Quantized LLM Worker running on :%s (Model: %s, Quant: %s)", port, modelName, quantType)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
}
