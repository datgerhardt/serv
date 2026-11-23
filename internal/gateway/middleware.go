package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"serv/internal/metrics"
	"serv/internal/models"
	"serv/internal/ratelimit"
	"serv/internal/storage"
)

// responseWriterInterceptor captures HTTP status code for metrics and logging
// while preserving http.Flusher support for SSE streaming.
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush forwards flushing to the underlying ResponseWriter when supported (required for SSE).
func (w *responseWriterInterceptor) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Unwrap exposes the underlying ResponseWriter to http.ResponseController.
func (w *responseWriterInterceptor) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// RequestIDHeader is the HTTP header for distributed tracing.
const RequestIDHeader = "X-Request-ID"

// MetricsMiddleware records Prometheus request metrics and TTFT.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		reqID := r.Header.Get(RequestIDHeader)
		if reqID == "" {
			reqID = uuid.New().String()
			r.Header.Set(RequestIDHeader, reqID)
		}
		w.Header().Set(RequestIDHeader, reqID)

		interceptor := &responseWriterInterceptor{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(interceptor, r)

		duration := time.Since(start).Seconds()
		path := r.URL.Path
		method := r.Method
		statusStr := strconv.Itoa(interceptor.statusCode)

		metrics.HTTPRequestsTotal.WithLabelValues(method, path, statusStr).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, path).Observe(duration)
	})
}

// RecoveryMiddleware prevents server panics from dropping connections.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				http.Error(w, `{"error":{"message":"Internal server error","type":"internal_error"}}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// RateLimitMiddleware enforces token-bucket / sliding-window rate limits per API key or client IP.
func RateLimitMiddleware(limiter ratelimit.Limiter, auditWriter *storage.AsyncAuditWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip health and metrics probes from rate limiting
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			clientKey := extractClientIdentity(r)
			res, err := limiter.Allow(r.Context(), clientKey)
			if err != nil {
				// On rate limiter failure, log error and allow request to prevent hard outage
				next.ServeHTTP(w, r)
				return
			}

			// Expose standard rate limit headers
			resetSec := int(res.ResetAfter.Seconds())
			if resetSec <= 0 {
				resetSec = 1
			}
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSec))

			if !res.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(resetSec))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)

				identityType := "ip"
				if strings.HasPrefix(clientKey, "key:") {
					identityType = "api_key"
				}
				metrics.RateLimitExceededTotal.WithLabelValues(identityType).Inc()

				if auditWriter != nil {
					auditWriter.Enqueue(models.RequestMetadata{
						RequestID:    r.Header.Get(RequestIDHeader),
						Model:        "unknown",
						ClientIP:     clientKey,
						StatusCode:   http.StatusTooManyRequests,
						ErrorMessage: fmt.Sprintf("Rate limit exceeded for %s", clientKey),
						CreatedAt:    time.Now(),
					})
				}

				respJSON := fmt.Sprintf(`{"error":{"message":"Rate limit reached (%d RPM). Please wait %d seconds before retrying.","type":"rate_limit_exceeded","param":null,"code":429}}`, res.Limit, resetSec)
				_, _ = w.Write([]byte(respJSON))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// extractClientIdentity derives an authenticated API key hash or the origin client IP.
func extractClientIdentity(r *http.Request) string {
	// 1. Bearer Token
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		h := sha256.Sum256([]byte(token))
		return "key:" + hex.EncodeToString(h[:8])
	}

	// 2. X-API-Key Header
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		h := sha256.Sum256([]byte(apiKey))
		return "key:" + hex.EncodeToString(h[:8])
	}

	// 3. X-Forwarded-For
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return "ip:" + ip
		}
	}

	// 4. X-Real-IP
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return "ip:" + strings.TrimSpace(realIP)
	}

	// 5. RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return "ip:" + host
	}

	return "ip:" + r.RemoteAddr
}
