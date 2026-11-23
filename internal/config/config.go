package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration parameters.
type Config struct {
	// Server
	ServerPort string
	LogLevel   string

	// Dynamic Batching
	MaxBatchSize  int
	BatchTimeout  time.Duration
	QueueCapacity int

	// Inference Backend
	BackendURL      string
	BackendTimeout  time.Duration
	ModelName       string
	QuantizationType string // AWQ, GPTQ, GGUF

	// Redis Cache
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	CacheTTL      time.Duration
	CacheEnabled  bool

	// PostgreSQL
	PostgresDSN           string
	PostgresMaxConns      int
	PostgresBufferCapacity int
	PostgresFlushInterval time.Duration
	AuditLoggingEnabled   bool

	// Rate Limiting
	RateLimitEnabled bool
	RateLimitRPM     int
	RateLimitBurst   int
}

// LoadFromEnv loads configuration parameters with sensible defaults.
func LoadFromEnv() *Config {
	return &Config{
		ServerPort:             getEnv("SERVER_PORT", "8080"),
		LogLevel:               getEnv("LOG_LEVEL", "info"),
		MaxBatchSize:           getEnvAsInt("MAX_BATCH_SIZE", 16),
		BatchTimeout:           getEnvAsDuration("BATCH_TIMEOUT_MS", 20*time.Millisecond),
		QueueCapacity:          getEnvAsInt("QUEUE_CAPACITY", 1024),
		BackendURL:             getEnv("BACKEND_URL", "http://localhost:8001"),
		BackendTimeout:         getEnvAsDuration("BACKEND_TIMEOUT_MS", 30*time.Second),
		ModelName:              getEnv("MODEL_NAME", "/Users/gh/models/llama3.2.gguf"),
		QuantizationType:       getEnv("QUANTIZATION_TYPE", "GGUF-Q4_K_M"),
		RedisAddr:              getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:          getEnv("REDIS_PASSWORD", ""),
		RedisDB:                getEnvAsInt("REDIS_DB", 0),
		CacheTTL:               getEnvAsDuration("CACHE_TTL_MINUTES", 60*time.Minute),
		CacheEnabled:           getEnvAsBool("CACHE_ENABLED", true),
		PostgresDSN:            getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/llm_metadata?sslmode=disable"),
		PostgresMaxConns:      getEnvAsInt("POSTGRES_MAX_CONNS", 25),
		PostgresBufferCapacity: getEnvAsInt("POSTGRES_BUFFER_CAPACITY", 5000),
		PostgresFlushInterval: getEnvAsDuration("POSTGRES_FLUSH_INTERVAL_MS", 1000*time.Millisecond),
		AuditLoggingEnabled:   getEnvAsBool("AUDIT_LOGGING_ENABLED", true),
		RateLimitEnabled:      getEnvAsBool("RATE_LIMIT_ENABLED", true),
		RateLimitRPM:          getEnvAsInt("RATE_LIMIT_RPM", 120),
		RateLimitBurst:        getEnvAsInt("RATE_LIMIT_BURST", 20),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvAsBool(key string, defaultVal bool) bool {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		return defaultVal
	}
	return val
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultVal
	}
	valInt, err := strconv.Atoi(valStr)
	if err == nil {
		// Treated as milliseconds if integer
		return time.Duration(valInt) * time.Millisecond
	}
	d, err := time.ParseDuration(valStr)
	if err != nil {
		return defaultVal
	}
	return d
}
