"""
Generate synthetic GPU training job events.

Simulates the kind of telemetry an AI cloud (CoreWeave, Lambda Labs, Together AI)
would collect from their job scheduler. Each record represents one training run.

Output: data/raw/job_events/job_events_<date>.jsonl
"""

import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import numpy as np

# ── Realistic distributions from MLCommons / Kubernetes scheduling research ──
GPU_TYPES = {
    "H100-SXM5-80GB": {"weight": 0.15, "price_per_hour": 4.25},
    "A100-SXM4-80GB": {"weight": 0.35, "price_per_hour": 3.00},
    "A100-PCIe-40GB":  {"weight": 0.25, "price_per_hour": 2.21},
    "RTX-4090":        {"weight": 0.15, "price_per_hour": 0.74},
    "A10G":            {"weight": 0.10, "price_per_hour": 0.90},
}

FRAMEWORKS = ["pytorch", "jax", "tensorflow", "deepspeed"]
FRAMEWORK_WEIGHTS = [0.55, 0.20, 0.15, 0.10]

MODEL_ARCHS = [
    "llm-7b", "llm-13b", "llm-70b",
    "diffusion-xl", "diffusion-base",
    "vit-large", "bert-large",
    "custom",
]
MODEL_WEIGHTS = [0.20, 0.15, 0.10, 0.15, 0.10, 0.10, 0.10, 0.10]

GPU_COUNTS = [1, 2, 4, 8, 16, 32, 64]
GPU_COUNT_WEIGHTS = [0.30, 0.20, 0.20, 0.15, 0.08, 0.05, 0.02]

ORGS = [f"org-{i:03d}" for i in range(1, 21)]
EXIT_CODES = [0, 0, 0, 0, 0, 1, 137, 143]   # mostly success, some OOM (137)


def _sample_duration_hours(gpu_count: int, model_arch: str) -> float:
    """
    Duration follows a log-normal distribution.
    Larger models × more GPUs → longer runs on average.
    """
    base_hours = {
        "llm-7b": 2.0, "llm-13b": 5.0, "llm-70b": 20.0,
        "diffusion-xl": 4.0, "diffusion-base": 1.5,
        "vit-large": 3.0, "bert-large": 2.0, "custom": 3.0,
    }
    mu = np.log(base_hours.get(model_arch, 3.0)) - 0.1 * np.log(gpu_count)
    sigma = 0.6
    return max(0.1, np.random.lognormal(mu, sigma))


def _sample_dataset_gb(model_arch: str) -> float:
    sizes = {
        "llm-7b": 500, "llm-13b": 800, "llm-70b": 2000,
        "diffusion-xl": 300, "diffusion-base": 150,
        "vit-large": 200, "bert-large": 120, "custom": 100,
    }
    base = sizes.get(model_arch, 100)
    return round(base * random.uniform(0.5, 2.0), 1)


def generate_jobs(n_jobs: int = 50_000, days_back: int = 90) -> list[dict]:
    random.seed(42)
    np.random.seed(42)

    gpu_types = list(GPU_TYPES.keys())
    gpu_weights = [GPU_TYPES[g]["weight"] for g in gpu_types]

    jobs = []
    now = datetime.utcnow()

    for _ in range(n_jobs):
        gpu_type = random.choices(gpu_types, weights=gpu_weights)[0]
        gpu_count = random.choices(GPU_COUNTS, weights=GPU_COUNT_WEIGHTS)[0]
        framework = random.choices(FRAMEWORKS, weights=FRAMEWORK_WEIGHTS)[0]
        model_arch = random.choices(MODEL_ARCHS, weights=MODEL_WEIGHTS)[0]
        org_id = random.choice(ORGS)
        user_id = f"user-{random.randint(1, 200):04d}"
        exit_code = random.choice(EXIT_CODES)

        started_at = now - timedelta(
            days=random.uniform(0, days_back),
            seconds=random.uniform(0, 86400),
        )
        duration_hours = _sample_duration_hours(gpu_count, model_arch)
        ended_at = started_at + timedelta(hours=duration_hours)

        price_per_hour = GPU_TYPES[gpu_type]["price_per_hour"]
        cost_usd = round(duration_hours * gpu_count * price_per_hour, 4)

        # Simulate late-arriving completion events:
        # ~15% of jobs have ended_at arrive separately (set to None here,
        # the producer sends a follow-up event 1-6h later)
        late_arrival = random.random() < 0.15
        job = {
            "job_id": str(uuid4()),
            "org_id": org_id,
            "user_id": user_id,
            "gpu_type": gpu_type,
            "gpu_count": gpu_count,
            "framework": framework,
            "model_arch": model_arch,
            "dataset_size_gb": _sample_dataset_gb(model_arch),
            "started_at": started_at.isoformat(),
            "ended_at": None if late_arrival else ended_at.isoformat(),
            "duration_hours": None if late_arrival else round(duration_hours, 4),
            "exit_code": None if late_arrival else exit_code,
            "cost_usd": None if late_arrival else cost_usd,
            "is_late_arrival": late_arrival,
            # Completion event payload (sent later by producer)
            "_completion": {
                "ended_at": ended_at.isoformat(),
                "duration_hours": round(duration_hours, 4),
                "exit_code": exit_code,
                "cost_usd": cost_usd,
                "late_delay_hours": round(random.uniform(1, 6), 2),
            } if late_arrival else None,
        }
        jobs.append(job)

    return jobs


def main():
    out_dir = Path(__file__).parents[2] / "data" / "raw" / "job_events"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating GPU job events...")
    jobs = generate_jobs(n_jobs=50_000, days_back=90)

    date_str = datetime.utcnow().strftime("%Y%m%d")
    out_path = out_dir / f"job_events_{date_str}.jsonl"

    with open(out_path, "w") as f:
        for job in jobs:
            # Strip the _completion helper before writing the start event
            record = {k: v for k, v in job.items() if k != "_completion"}
            f.write(json.dumps(record) + "\n")

    # Write completion events to a separate file (late arrivals)
    completions = [j["_completion"] | {"job_id": j["job_id"]}
                   for j in jobs if j["is_late_arrival"]]
    comp_path = out_dir / f"job_completions_{date_str}.jsonl"
    with open(comp_path, "w") as f:
        for c in completions:
            f.write(json.dumps(c) + "\n")

    print(f"  {len(jobs):,} start events   → {out_path}")
    print(f"  {len(completions):,} completion events → {comp_path}")
    print(f"  Late-arrival rate: {len(completions)/len(jobs):.1%}")


if __name__ == "__main__":
    main()
