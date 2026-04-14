"""
Generate synthetic GPU node utilization metrics (time-series).

Simulates per-GPU-per-hour utilization readings from a 96-node cluster.
Used for capacity planning and SLA monitoring dashboards.

Output: data/raw/node_metrics/node_metrics_<date>.csv
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

N_NODES = 96
GPUS_PER_NODE = 8
GPU_TYPES_BY_NODE = {
    **{f"node-{i:03d}": "H100-SXM5-80GB" for i in range(1, 17)},    # 16 H100 nodes
    **{f"node-{i:03d}": "A100-SXM4-80GB" for i in range(17, 65)},   # 48 A100 nodes
    **{f"node-{i:03d}": "RTX-4090"        for i in range(65, 97)},   # 32 RTX nodes
}

RACK_ASSIGNMENT = {f"node-{i:03d}": f"rack-{((i - 1) // 8) + 1:02d}"
                   for i in range(1, N_NODES + 1)}


def _utilization_profile(hour_of_day: int) -> float:
    """
    Cluster utilization follows a circadian pattern.
    Peak during US business hours (14:00–22:00 UTC = 09:00–17:00 ET).
    """
    if 14 <= hour_of_day <= 22:
        return random.uniform(0.75, 0.95)
    elif 8 <= hour_of_day <= 14:
        return random.uniform(0.55, 0.80)
    else:
        return random.uniform(0.30, 0.60)   # overnight batch jobs


def generate_node_metrics(days_back: int = 90) -> list[dict]:
    random.seed(99)
    np.random.seed(99)

    nodes = list(GPU_TYPES_BY_NODE.keys())
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days_back)

    records = []
    current = start
    while current <= now:
        hour_util = _utilization_profile(current.hour)

        for node_id in nodes:
            gpu_type = GPU_TYPES_BY_NODE[node_id]
            rack_id = RACK_ASSIGNMENT[node_id]

            # Node-level power draw baseline (watts)
            tdp = {"H100-SXM5-80GB": 700, "A100-SXM4-80GB": 400, "RTX-4090": 450}[gpu_type]

            for gpu_idx in range(GPUS_PER_NODE):
                gpu_id = f"{node_id}-gpu-{gpu_idx}"
                # Per-GPU util varies around node average
                gpu_util = min(100.0, max(0.0,
                    hour_util * 100 + np.random.normal(0, 8)))
                memory_util = min(100.0, max(0.0,
                    gpu_util * random.uniform(0.6, 1.1) + np.random.normal(0, 5)))
                temp_c = 35 + (gpu_util / 100) * 45 + np.random.normal(0, 3)
                power_w = tdp * (0.3 + 0.7 * (gpu_util / 100)) + np.random.normal(0, 15)

                records.append({
                    "timestamp": current.isoformat(),
                    "node_id": node_id,
                    "gpu_id": gpu_id,
                    "gpu_index": gpu_idx,
                    "gpu_type": gpu_type,
                    "rack_id": rack_id,
                    "gpu_util_pct": round(gpu_util, 2),
                    "memory_util_pct": round(memory_util, 2),
                    "temp_celsius": round(temp_c, 1),
                    "power_watts": round(max(0, power_w), 1),
                })

        current += timedelta(hours=1)

    return records


def main():
    out_dir = Path(__file__).parents[2] / "data" / "raw" / "node_metrics"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating node metrics for {N_NODES} nodes × {GPUS_PER_NODE} GPUs × 90 days...")
    records = generate_node_metrics(days_back=90)

    date_str = datetime.utcnow().strftime("%Y%m%d")
    out_path = out_dir / f"node_metrics_{date_str}.csv"

    fieldnames = list(records[0].keys())
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"  {len(records):,} rows → {out_path}")
    total_gb = out_path.stat().st_size / 1024 / 1024 / 1024
    print(f"  File size: {total_gb:.2f} GB")


if __name__ == "__main__":
    main()
