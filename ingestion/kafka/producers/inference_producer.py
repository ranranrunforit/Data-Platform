"""
Kafka producer for inference API request logs.

Publishes to topic: inference-api-logs

Can run in two modes:
  --historical  : bulk-loads the generated JSONL file (for seeding)
  --live        : continuously generates and publishes ~100 req/s (for streaming demo)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Add /opt to path so `from data.generator...` resolves to /opt/data/generator
sys.path.insert(0, str(Path(__file__).parents[3]))
from data.generator.inference_logs import generate_inference_logs

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "inference-api-logs"


def _build_producer(retries: int = 10) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,                  # leader ack only (inference logs can tolerate this)
                linger_ms=5,
                batch_size=32 * 1024,
            )
        except NoBrokersAvailable:
            print(f"  Broker not ready (attempt {attempt}/{retries}), retrying in 5s...")
            time.sleep(5)
    print("Could not connect to Kafka. Exiting.")
    sys.exit(1)


def publish_historical(producer: KafkaProducer, data_dir: Path) -> None:
    """Bulk-load existing JSONL files into Kafka."""
    files = sorted(data_dir.glob("inference_logs_*.jsonl"))
    if not files:
        print(f"No inference log files in {data_dir}. Run: make generate")
        return

    total = 0
    for path in files:
        print(f"Publishing {path.name}...")
        with open(path) as f:
            for line in f:
                record = json.loads(line)
                producer.send(TOPIC, key=record["request_id"], value=record)
                total += 1
                if total % 10_000 == 0:
                    producer.flush()
                    print(f"  {total:,} records sent...")

    producer.flush()
    print(f"Historical load complete: {total:,} records → topic '{TOPIC}'")


def publish_live(producer: KafkaProducer, rate_per_sec: int = 100) -> None:
    """
    Continuously generate and publish synthetic inference requests.
    Demonstrates live streaming for the Spark Structured Streaming consumer.
    Press Ctrl+C to stop.
    """
    print(f"Live mode: publishing ~{rate_per_sec} requests/sec. Ctrl+C to stop.")
    sent = 0
    interval = 1.0 / rate_per_sec

    try:
        while True:
            # Generate a small batch (1 request at a time for realism)
            record = generate_inference_logs(n_requests=1, days_back=0)[0]
            record["timestamp"] = datetime.utcnow().isoformat()   # real-time timestamp
            producer.send(TOPIC, key=record["request_id"], value=record)
            sent += 1

            if sent % (rate_per_sec * 10) == 0:
                producer.flush()
                print(f"  [{datetime.utcnow().strftime('%H:%M:%S')}] {sent:,} live requests sent")

            time.sleep(interval)

    except KeyboardInterrupt:
        producer.flush()
        print(f"\nStopped. Total sent: {sent:,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true",
                        help="Continuously generate and publish (streaming demo)")
    parser.add_argument("--rate", type=int, default=100,
                        help="Requests per second in live mode (default: 100)")
    args = parser.parse_args()

    data_dir = Path(__file__).parents[4] / "data" / "raw" / "inference_logs"
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    producer = _build_producer()
    print("Connected.")

    if args.live:
        publish_live(producer, rate_per_sec=args.rate)
    else:
        publish_historical(producer, data_dir)

    producer.close()


if __name__ == "__main__":
    main()
