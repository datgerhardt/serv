package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"serv/internal/models"
)

// redactDSN strips credentials from a PostgreSQL DSN before logging.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "<redacted-dsn>"
	}
	return u.Redacted()
}

// PostgresStore manages persistent metadata and telemetry logs in PostgreSQL.
type PostgresStore struct {
	db      *sql.DB
	enabled bool
}

// NewPostgresStore creates a connection pool and initializes tables.
func NewPostgresStore(dsn string, maxConns int, enabled bool) (*PostgresStore, error) {
	if !enabled {
		log.Println("[Storage] PostgreSQL metadata and audit logging is disabled via configuration")
		return &PostgresStore{enabled: false}, nil
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("[Storage] Error connecting to PostgreSQL: %v (falling back to no-op)", err)
		return &PostgresStore{enabled: false}, nil
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Printf("[Storage] Warning: PostgreSQL ping failed at %s: %v (will operate in degraded mode)", redactDSN(dsn), err)
		return &PostgresStore{db: db, enabled: false}, nil
	}

	store := &PostgresStore{
		db:      db,
		enabled: true,
	}

	if err := store.initSchema(ctx); err != nil {
		log.Printf("[Storage] Failed to initialize PostgreSQL schema: %v", err)
		return store, err
	}

	log.Printf("[Storage] Successfully connected to PostgreSQL (MaxConns: %d)", maxConns)
	return store, nil
}

// initSchema applies required tables and performance indexes.
func (s *PostgresStore) initSchema(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS request_logs (
		id BIGSERIAL PRIMARY KEY,
		request_id VARCHAR(64) NOT NULL,
		model VARCHAR(128) NOT NULL,
		prompt_tokens INT NOT NULL DEFAULT 0,
		completion_tokens INT NOT NULL DEFAULT 0,
		total_tokens INT NOT NULL DEFAULT 0,
		latency_ms BIGINT NOT NULL DEFAULT 0,
		ttft_ms BIGINT NOT NULL DEFAULT 0,
		cached BOOLEAN NOT NULL DEFAULT FALSE,
		client_ip VARCHAR(64),
		status_code INT NOT NULL DEFAULT 200,
		error_message TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_request_logs_model ON request_logs(model);
	CREATE INDEX IF NOT EXISTS idx_request_logs_request_id ON request_logs(request_id);
	`
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// InsertBatch performs an optimized multi-row INSERT into PostgreSQL.
func (s *PostgresStore) InsertBatch(ctx context.Context, records []models.RequestMetadata) error {
	if s == nil || !s.enabled || s.db == nil || len(records) == 0 {
		return nil
	}

	var valStrings []string
	var valArgs []interface{}
	argIdx := 1

	for _, rec := range records {
		valStrings = append(valStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5, argIdx+6, argIdx+7, argIdx+8, argIdx+9, argIdx+10, argIdx+11))

		valArgs = append(valArgs,
			rec.RequestID,
			rec.Model,
			rec.PromptTokens,
			rec.CompletionTokens,
			rec.TotalTokens,
			rec.Latency.Milliseconds(),
			rec.TTFT.Milliseconds(),
			rec.Cached,
			rec.ClientIP,
			rec.StatusCode,
			rec.ErrorMessage,
			rec.CreatedAt,
		)
		argIdx += 12
	}

	stmt := fmt.Sprintf(`
		INSERT INTO request_logs (
			request_id, model, prompt_tokens, completion_tokens, total_tokens,
			latency_ms, ttft_ms, cached, client_ip, status_code, error_message, created_at
		) VALUES %s`, strings.Join(valStrings, ","))

	_, err := s.db.ExecContext(ctx, stmt, valArgs...)
	if err != nil {
		log.Printf("[Storage] Error executing batch insert of %d records: %v", len(records), err)
		return err
	}

	return nil
}

// Close closes the database connection pool.
func (s *PostgresStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
