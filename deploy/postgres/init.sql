-- PostgreSQL Schema for High-Scale Quantized LLM Serving
-- Includes request logging, token accounting, and latency performance tracking

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

-- Optimize search and aggregation queries
CREATE INDEX IF NOT EXISTS idx_request_logs_created_at ON request_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_request_logs_model ON request_logs(model);
CREATE INDEX IF NOT EXISTS idx_request_logs_request_id ON request_logs(request_id);
CREATE INDEX IF NOT EXISTS idx_request_logs_status_cached ON request_logs(status_code, cached);

-- Aggregation view for token consumption and latency metrics
CREATE OR REPLACE VIEW v_hourly_token_usage AS
SELECT 
    date_trunc('hour', created_at) AS hour,
    model,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN cached THEN 1 ELSE 0 END) AS cache_hits,
    SUM(prompt_tokens) AS sum_prompt_tokens,
    SUM(completion_tokens) AS sum_completion_tokens,
    AVG(latency_ms)::numeric(10,2) AS avg_latency_ms,
    AVG(ttft_ms)::numeric(10,2) AS avg_ttft_ms
FROM request_logs
GROUP BY 1, 2
ORDER BY 1 DESC;
