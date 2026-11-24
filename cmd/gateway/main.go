package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"serv/internal/backend"
	"serv/internal/batcher"
	"serv/internal/cache"
	"serv/internal/config"
	"serv/internal/gateway"
	"serv/internal/ratelimit"
	"serv/internal/storage"
)

func main() {
	log.Println("Starting Quantized LLM High-Scale Serving Gateway")

	cfg := config.LoadFromEnv()

	// 1. Initialize Redis Cache
	redisCache, err := cache.NewRedisCache(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
		cfg.CacheTTL,
		cfg.CacheEnabled,
	)
	if err != nil {
		log.Printf("Redis init warning: %v", err)
	}
	defer redisCache.Close()

	// 2. Initialize Distributed Rate Limiter
	rateLimiter := ratelimit.NewLimiter(
		redisCache.Client(),
		cfg.RateLimitRPM,
		cfg.RateLimitEnabled,
	)
	defer rateLimiter.Close()
	log.Printf("Rate limiter initialized (Enabled: %v, RPM: %d)", cfg.RateLimitEnabled, cfg.RateLimitRPM)

	// 3. Initialize PostgreSQL Storage & Async Audit Writer
	pgStore, err := storage.NewPostgresStore(
		cfg.PostgresDSN,
		cfg.PostgresMaxConns,
		cfg.AuditLoggingEnabled,
	)
	if err != nil {
		log.Printf("Postgres init warning: %v", err)
	}
	defer pgStore.Close()

	auditWriter := storage.NewAsyncAuditWriter(
		pgStore,
		cfg.PostgresBufferCapacity,
		100, // batch insert size
		cfg.PostgresFlushInterval,
	)
	defer auditWriter.Stop()

	// 4. Initialize Backend LLM Client & Dynamic Batching Engine
	backendClient := backend.NewClient(cfg.BackendURL, cfg.BackendTimeout)

	batchEngine := batcher.NewDynamicBatcher(
		backendClient,
		cfg.MaxBatchSize,
		cfg.BatchTimeout,
		cfg.QueueCapacity,
		16, // max concurrent batches
		cfg.ModelName,
	)
	defer batchEngine.Stop()

	// 5. Initialize HTTP Server
	h := gateway.NewHandler(
		batchEngine,
		redisCache,
		auditWriter,
		backendClient,
		cfg.ModelName,
		cfg.QuantizationType,
	)

	srv := gateway.NewServer(cfg.ServerPort, h, rateLimiter, auditWriter)

	// 5. Start Server in Goroutine
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server startup error: %v", err)
		}
	}()

	// 6. Graceful Shutdown on SIGINT / SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Termination signal received. Commencing graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during HTTP server shutdown: %v", err)
	}

	log.Println("Gateway cleanly stopped. Exiting.")
}
