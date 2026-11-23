package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPMetrics
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "llm_http_requests_total",
			Help: "Total number of HTTP requests processed by the gateway",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "llm_http_request_duration_seconds",
			Help:    "Histogram of HTTP request durations in seconds",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0},
		},
		[]string{"method", "path"},
	)

	// Batching Metrics
	BatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "llm_batch_size",
			Help:    "Size of batched inference requests dispatched to backend",
			Buckets: []float64{1, 2, 4, 8, 16, 24, 32, 48, 64},
		},
	)

	BatchProcessingDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "llm_batch_processing_duration_seconds",
			Help:    "Time taken to execute a batch on the quantized backend",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0},
		},
	)

	QueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "llm_queue_depth",
			Help: "Current number of requests queued waiting for batching",
		},
	)

	ActiveInferences = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "llm_active_inferences",
			Help: "Number of batches/inferences currently running on quantized workers",
		},
	)

	// Redis Cache Metrics
	CacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "llm_cache_hits_total",
			Help: "Total number of exact/prompt cache hits in Redis",
		},
	)

	CacheMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "llm_cache_misses_total",
			Help: "Total number of cache misses requiring model inference",
		},
	)

	CacheLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "llm_cache_lookup_duration_seconds",
			Help:    "Latency of Redis cache lookups and writes",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05},
		},
	)

	// Token & Latency Metrics
	PromptTokensTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "llm_prompt_tokens_total",
			Help: "Total number of prompt tokens processed",
		},
		[]string{"model"},
	)

	CompletionTokensTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "llm_completion_tokens_total",
			Help: "Total number of generated completion tokens",
		},
		[]string{"model"},
	)

	TimeToFirstToken = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "llm_time_to_first_token_seconds",
			Help:    "Time to first token (TTFT) for streaming responses",
			Buckets: []float64{0.02, 0.05, 0.1, 0.2, 0.4, 0.8, 1.5, 3.0, 5.0},
		},
		[]string{"model"},
	)

	// Rate Limiting Metrics
	RateLimitExceededTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "llm_rate_limit_exceeded_total",
			Help: "Total number of HTTP requests rejected due to rate limit violation",
		},
		[]string{"client_id"},
	)
)
