package storage

import (
	"strings"
	"testing"
)

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name      string
		dsn       string
		forbidden string
		expected  string
	}{
		{
			name:      "redacts password in postgres URL",
			dsn:       "postgres://llm_user:SuperSecretPass!@db.internal:5432/llm_metadata?sslmode=require",
			forbidden: "SuperSecretPass!",
			expected:  "postgres://llm_user:xxxxx@db.internal:5432/llm_metadata?sslmode=require",
		},
		{
			name:      "keeps URL without password intact",
			dsn:       "postgres://db.internal:5432/llm_metadata",
			forbidden: "",
			expected:  "postgres://db.internal:5432/llm_metadata",
		},
		{
			name:      "handles key-value DSN safely",
			dsn:       "host=localhost user=postgres password=topsecret dbname=llm_metadata",
			forbidden: "topsecret",
			expected:  "<redacted-dsn>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := redactDSN(tc.dsn)
			if tc.forbidden != "" && strings.Contains(got, tc.forbidden) {
				t.Fatalf("redactDSN leaked secret %q in %q", tc.forbidden, got)
			}
			if got != tc.expected {
				t.Errorf("Expected %q, got %q", tc.expected, got)
			}
		})
	}
}
