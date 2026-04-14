"""
Kafka producer for GPU training job events.

Reads the generated JSONL files and publishes to two topics:
  - gpu-job-events      : job start events
  - gpu-job-completions : completion events (simulates late arrivals)

In a real system this would be your job scheduler (SLURM / Kubernetes)
emitting events via a webhook or log shipper.
"""

import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_JOBS = "gpu-job-events"
TOPIC_COMPLETIONS = "gpu-job-completions"


def _build_producer(retries: int = 10) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",              # strongest durability guarantee
                retries=3,
                linger_ms=10,            # micro-batch up to 10ms for throughput
                batch_size=64 * 1024,    # 64KB batches
            )
        except NoBrokersAvailable:
            print(f"  Broker not ready (attempt {attempt}/{retries}), retrying in 5s...")
            time.sleep(5)
    print("Could not connect to Kafka. Exiting.")
    sys.exit(1)


def publish_jobs(producer: KafkaProducer, data_dir: Path) -> None:
    job_files = sorted(data_dir.glob("job_events_*.jsonl"))
    comp_files = sorted(data_dir.glob("job_completions_*.jsonl"))

    if not job_files:
        print(f"No job event files found in {data_dir}. Run: make generate")
        return

    total_sent = 0
    for path in job_files:
        print(f"Publishing {path.name}...")
        with open(path) as f:
            for line in f:
                event = json.loads(line)
                producer.send(
                    TOPIC_JOBS,
                    key=event["job_id"],
                    value=event,
                )
                total_sent += 1
                if total_sent % 5_000 == 0:
                    producer.flush()
                    print(f"  {total_sent:,} job events sent...")

    producer.flush()
    print(f"Job events done: {total_sent:,} records → topic '{TOPIC_JOBS}'")

    # Completion events (late arrivals — published with a simulated delay)
    comp_sent = 0
    for path in comp_files:
        print(f"Publishing completion events: {path.name}...")
        with open(path) as f:
            for line in f:
                event = json.loads(line)
                producer.send(
                    TOPIC_COMPLETIONS,
                    key=event["job_id"],
                    value=event,
                )
                comp_sent += 1

    producer.flush()
    print(f"Completion events done: {comp_sent:,} records → topic '{TOPIC_COMPLETIONS}'")


def main():
    data_dir = Path(__file__).parents[4] / "data" / "raw" / "job_events"
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    producer = _build_producer()
    print("Connected.")
    publish_jobs(producer, data_dir)
    producer.close()
    print("Producer closed.")


if __name__ == "__main__":
    main()
