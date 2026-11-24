package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"serv/internal/ratelimit"
	"serv/internal/storage"
)

// Server wraps the HTTP listener and router.
type Server struct {
	httpServer *http.Server
	handler    *Handler
}

// NewServer configures routes, middleware, and http.Server parameters.
func NewServer(port string, h *Handler, limiter ratelimit.Limiter, auditWriter *storage.AsyncAuditWriter) *Server {
	r := mux.NewRouter()

	// Global Middleware
	r.Use(RecoveryMiddleware)
	r.Use(MetricsMiddleware)
	if limiter != nil {
		r.Use(RateLimitMiddleware(limiter, auditWriter))
	}

	// API Endpoints
	r.HandleFunc("/v1/chat/completions", h.HandleChatCompletions).Methods(http.MethodPost)
	r.HandleFunc("/v1/models", h.HandleModels).Methods(http.MethodGet)

	// Health & Observability
	r.HandleFunc("/healthz", h.HandleHealthz).Methods(http.MethodGet)
	r.HandleFunc("/readyz", h.HandleReadyz).Methods(http.MethodGet)
	r.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return &Server{
		httpServer: srv,
		handler:    h,
	}
}

// Start runs the HTTP server.
func (s *Server) Start() error {
	log.Printf("HTTP API Gateway listening on %s", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server failure: %w", err)
	}
	return nil
}

// Shutdown initiates graceful termination.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down HTTP API Gateway...")
	return s.httpServer.Shutdown(ctx)
}
