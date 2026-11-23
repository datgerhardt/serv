#!/usr/bin/env bash
# High-Scale LLM Serving Benchmark & Validation Script
# Tests dynamic batching, Redis caching acceleration, and streaming endpoints.

set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
CONCURRENCY=16
TOTAL_REQUESTS=64

echo "=========================================================="
echo "  High-Scale Quantized LLM Serving Benchmark"
echo "  Target: ${GATEWAY_URL}"
echo "=========================================================="

MODEL_NAME="${MODEL_NAME:-/Users/gh/models/llama3.2.gguf}"

# Check health
echo -n "[1/4] Checking gateway health... "
HEALTH=$(curl -s "${GATEWAY_URL}/healthz")
echo "OK (${HEALTH})"

# Check models
echo -n "[2/4] Querying available quantized models... "
MODELS=$(curl -s "${GATEWAY_URL}/v1/models")
echo "${MODELS}"

# Test 1: Single request (Cold Inference / Cache MISS)
echo ""
echo "[3/4] Testing Cold Request vs. Redis Cache HIT..."
PAYLOAD='{
  "model": "'"${MODEL_NAME}"'",
  "messages": [{"role": "user", "content": "Explain quantized Large Language Model inference on Apple Silicon Metal."}],
  "max_tokens": 64
}'

START_TIME=$(python3 -c "import time; print(time.time())")
COLD_RESP=$(curl -s -i -X POST "${GATEWAY_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")
END_TIME=$(python3 -c "import time; print(time.time())")
COLD_DURATION=$(python3 -c "print(f'{(${END_TIME} - ${START_TIME}) * 1000:.2f}')")
CACHE_HEADER=$(echo "${COLD_RESP}" | grep -i "X-Cache-Status:" | tr -d '\r' || true)
RATELIMIT_LIMIT=$(echo "${COLD_RESP}" | grep -i "X-RateLimit-Limit:" | tr -d '\r' || true)
RATELIMIT_REMAINING=$(echo "${COLD_RESP}" | grep -i "X-RateLimit-Remaining:" | tr -d '\r' || true)

echo "  -> Cold Request Latency : ${COLD_DURATION} ms (${CACHE_HEADER})"
if [ -n "${RATELIMIT_LIMIT}" ]; then
  echo "  -> Rate Limit Status    : ${RATELIMIT_LIMIT}, ${RATELIMIT_REMAINING}"
fi

# Test 2: Repeat request (Redis Cache HIT)
START_TIME=$(python3 -c "import time; print(time.time())")
WARM_RESP=$(curl -s -i -X POST "${GATEWAY_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")
END_TIME=$(python3 -c "import time; print(time.time())")
WARM_DURATION=$(python3 -c "print(f'{(${END_TIME} - ${START_TIME}) * 1000:.2f}')")
CACHE_HEADER=$(echo "${WARM_RESP}" | grep -i "X-Cache-Status:" | tr -d '\r' || true)

echo "  -> Warm Request Latency : ${WARM_DURATION} ms (${CACHE_HEADER})"
echo "  -> Speedup Factor       : $(python3 -c "print(f'{float(${COLD_DURATION}) / max(float(${WARM_DURATION}), 0.1):.1f}x')")"

# Test 3: High Concurrency Dynamic Batching Benchmark
echo ""
echo "[4/4] Executing Dynamic Batching Load Test (${TOTAL_REQUESTS} requests, ${CONCURRENCY} concurrent)..."

start_load=$(python3 -c "import time; print(time.time())")

seq 1 ${TOTAL_REQUESTS} | xargs -n 1 -P ${CONCURRENCY} -I {} curl -s -X POST "${GATEWAY_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "X-Cache-Bypass: true" \
  -d '{"model":"'"${MODEL_NAME}"'","messages":[{"role":"user","content":"Batch request {}"}],"max_tokens":16}' > /dev/null

end_load=$(python3 -c "import time; print(time.time())")
total_seconds=$(python3 -c "print(f'{${end_load} - ${start_load}:.2f}')")
rps=$(python3 -c "print(f'{${TOTAL_REQUESTS} / max(${end_load} - ${start_load}, 0.001):.2f}')")

echo "  -> Completed in : ${total_seconds} s"
echo "  -> Throughput   : ${rps} requests/second"

echo ""
echo "=========================================================="
echo "  Prometheus Metrics Snapshot:"
curl -s "${GATEWAY_URL}/metrics" | grep -E "(llm_batch_size_sum|llm_batch_size_count|llm_cache_hits_total|llm_cache_misses_total)" || true
echo "=========================================================="
