"""
Generate synthetic inference API request logs.

Simulates logs from an LLM inference endpoint (like Together AI / Replicate).
Latency follows a bimodal distribution: fast cache hits + slow full generations.

Output: data/raw/inference_logs/inference_logs_<date>.jsonl
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import numpy as np

MODELS = {
    "llama-3-70b-instruct":    {"weight": 0.30, "cost_per_1k_tokens": 0.0009},
    "llama-3-8b-instruct":     {"weight": 0.25, "cost_per_1k_tokens": 0.0002},
    "mixtral-8x7b-instruct":   {"weight": 0.15, "cost_per_1k_tokens": 0.0006},
    "stable-diffusion-xl":     {"weight": 0.10, "cost_per_1k_tokens": 0.0400},
    "whisper-large-v3":        {"weight": 0.08, "cost_per_1k_tokens": 0.0060},
    "llama-3-405b-instruct":   {"weight": 0.07, "cost_per_1k_tokens": 0.0035},
    "codellama-34b-instruct":  {"weight": 0.05, "cost_per_1k_tokens": 0.0008},
}

REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
REGION_WEIGHTS = [0.40, 0.25, 0.25, 0.10]

STATUS_CODES = [200, 200, 200, 200, 200, 200, 200, 429, 500, 503]


def _sample_latency_ms(model: str, cache_hit: bool) -> float:
    """
    Bimodal latency: cache hits are fast (tens of ms),
    full generations are slow (hundreds to thousands of ms).
    """
    if cache_hit:
        return max(5.0, np.random.lognormal(mean=3.5, sigma=0.4))   # ~33ms median

    base_latency = {
        "llama-3-70b-instruct":   np.random.lognormal(6.5, 0.5),    # ~665ms
        "llama-3-8b-instruct":    np.random.lognormal(5.5, 0.4),    # ~245ms
        "mixtral-8x7b-instruct":  np.random.lognormal(6.2, 0.5),
        "stable-diffusion-xl":    np.random.lognormal(7.5, 0.4),    # ~1800ms
        "whisper-large-v3":       np.random.lognormal(6.8, 0.5),
        "llama-3-405b-instruct":  np.random.lognormal(7.2, 0.6),
        "codellama-34b-instruct": np.random.lognormal(6.4, 0.5),
    }
    return max(50.0, base_latency.get(model, np.random.lognormal(6.0, 0.5)))


def generate_inference_logs(n_requests: int = 500_000, days_back: int = 90) -> list[dict]:
    random.seed(123)
    np.random.seed(123)

    model_names = list(MODELS.keys())
    model_weights = [MODELS[m]["weight"] for m in model_names]
    now = datetime.utcnow()

    logs = []
    for _ in range(n_requests):
        model = random.choices(model_names, weights=model_weights)[0]
        region = random.choices(REGIONS, weights=REGION_WEIGHTS)[0]
        status = random.choice(STATUS_CODES)
        cache_hit = random.random() < 0.22   # 22% cache hit rate
        org_id = f"org-{random.randint(1, 20):03d}"
        user_id = f"user-{random.randint(1, 200):04d}"

        ts = now - timedelta(
            days=random.uniform(0, days_back),
            seconds=random.uniform(0, 86400),
        )

        input_tokens = int(np.random.lognormal(5.5, 0.8))    # ~245 median
        output_tokens = int(np.random.lognormal(4.5, 0.9))   # ~90 median
        total_tokens = input_tokens + output_tokens

        cost_per_1k = MODELS[model]["cost_per_1k_tokens"]
        cost_usd = round((total_tokens / 1000) * cost_per_1k, 6)

        latency_ms = _sample_latency_ms(model, cache_hit)
        if status != 200:
            latency_ms = round(random.uniform(10, 200), 1)   # errors are fast

        logs.append({
            "request_id": str(uuid4()),
            "org_id": org_id,
            "user_id": user_id,
            "model_id": model,
            "region": region,
            "status_code": status,
            "cache_hit": cache_hit,
            "input_tokens": input_tokens if status == 200 else 0,
            "output_tokens": output_tokens if status == 200 else 0,
            "total_tokens": total_tokens if status == 200 else 0,
            "latency_ms": round(latency_ms, 2),
            "cost_usd": cost_usd if status == 200 else 0.0,
            "timestamp": ts.isoformat(),
        })

    return logs


def main():
    out_dir = Path(__file__).parents[2] / "data" / "raw" / "inference_logs"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating inference API logs...")
    logs = generate_inference_logs(n_requests=500_000, days_back=90)

    date_str = datetime.utcnow().strftime("%Y%m%d")
    out_path = out_dir / f"inference_logs_{date_str}.jsonl"

    with open(out_path, "w") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")

    success = [l for l in logs if l["status_code"] == 200]
    print(f"  {len(logs):,} requests → {out_path}")
    print(f"  Success rate: {len(success)/len(logs):.1%}")
    latencies = [l["latency_ms"] for l in success]
    latencies.sort()
    p50 = latencies[int(0.50 * len(latencies))]
    p99 = latencies[int(0.99 * len(latencies))]
    print(f"  Latency p50={p50:.0f}ms  p99={p99:.0f}ms")


if __name__ == "__main__":
    main()
