#!/usr/bin/env python3
"""
Test and validate Server-Sent Events (SSE) streaming with the Go LLM Gateway.
Measures Time-to-First-Token (TTFT) and token generation velocity.
"""

import json
import os
import sys
import time
import urllib.request

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8080")
STREAM_ENDPOINT = f"{GATEWAY_URL}/v1/chat/completions"
MODEL_NAME = os.getenv("MODEL_NAME", "/Users/gh/models/llama3.2.gguf")

def test_stream():
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": "How do quantized LLMs scale with micro-batching?"}
        ],
        "stream": True,
        "max_tokens": 100,
    }

    print(f"Connecting to {STREAM_ENDPOINT}...")
    req = urllib.request.Request(
        STREAM_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    start_time = time.time()
    first_token_time = None
    token_count = 0

    print("\n--- Model Response Stream ---")
    with urllib.request.urlopen(req) as response:
        for line in response:
            line_str = line.decode("utf-8").strip()
            if not line_str or line_str.startswith(":"):
                continue

            if line_str.startswith("data: "):
                data = line_str[6:].strip()
                if data == "[DONE]":
                    break

                try:
                    chunk = json.loads(data)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        if first_token_time is None:
                            first_token_time = time.time()
                        token_count += 1
                        sys.stdout.write(content)
                        sys.stdout.flush()
                except json.JSONDecodeError:
                    continue

    end_time = time.time()
    total_duration = end_time - start_time
    ttft = (first_token_time - start_time) * 1000 if first_token_time else 0
    tps = token_count / max((end_time - (first_token_time or start_time)), 0.001)

    print("\n\n--- Performance Telemetry ---")
    print(f"Time to First Token (TTFT) : {ttft:.2f} ms")
    print(f"Total Tokens Emitted        : {token_count} tokens")
    print(f"Total Stream Duration       : {total_duration * 1000:.2f} ms")
    print(f"Token Generation Rate       : {tps:.2f} tokens/second")

if __name__ == "__main__":
    test_stream()
