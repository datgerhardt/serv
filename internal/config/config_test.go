package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnv_Defaults(t *testing.T) {
	t.Setenv("BACKEND_URL", "")
	t.Setenv("MODEL_NAME", "")
	t.Setenv("QUANTIZATION_TYPE", "")
	t.Setenv("CACHE_TTL_MINUTES", "")
	t.Setenv("BATCH_TIMEOUT_MS", "")

	cfg := LoadFromEnv()

	if cfg.BackendURL != "http://localhost:8000" {
		t.Errorf("Expected default BACKEND_URL http://localhost:8000, got %q", cfg.BackendURL)
	}
	if strings.HasPrefix(cfg.ModelName, "/Users/") {
		t.Errorf("Default MODEL_NAME should not contain a local developer path, got %q", cfg.ModelName)
	}
	if cfg.CacheTTL != 60*time.Minute {
		t.Errorf("Expected default CacheTTL 60m, got %v", cfg.CacheTTL)
	}
	if cfg.BatchTimeout != 20*time.Millisecond {
		t.Errorf("Expected default BatchTimeout 20ms, got %v", cfg.BatchTimeout)
	}
}

func TestLoadFromEnv_DurationUnits(t *testing.T) {
	t.Run("bare integer minutes for CACHE_TTL_MINUTES", func(t *testing.T) {
		t.Setenv("CACHE_TTL_MINUTES", "45")
		cfg := LoadFromEnv()
		if cfg.CacheTTL != 45*time.Minute {
			t.Errorf("Expected CacheTTL 45m for CACHE_TTL_MINUTES=45, got %v", cfg.CacheTTL)
		}
	})

	t.Run("explicit Go duration string for CACHE_TTL_MINUTES", func(t *testing.T) {
		t.Setenv("CACHE_TTL_MINUTES", "120m")
		cfg := LoadFromEnv()
		if cfg.CacheTTL != 120*time.Minute {
			t.Errorf("Expected CacheTTL 120m for CACHE_TTL_MINUTES=120m, got %v", cfg.CacheTTL)
		}
	})

	t.Run("bare integer milliseconds for BATCH_TIMEOUT_MS", func(t *testing.T) {
		t.Setenv("BATCH_TIMEOUT_MS", "35")
		cfg := LoadFromEnv()
		if cfg.BatchTimeout != 35*time.Millisecond {
			t.Errorf("Expected BatchTimeout 35ms for BATCH_TIMEOUT_MS=35, got %v", cfg.BatchTimeout)
		}
	})
}
