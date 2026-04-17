"""
Upload raw generated data files to MinIO bronze bucket.

Populates the three prefixes that bronze_to_silver.py expects:
  bronze/job_events/       ← job start events
  bronze/job_completions/  ← late-arrival completion events
  bronze/inference_logs/   ← inference API logs

Run via: make upload-bronze
"""
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
KEY      = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
RAW_DIR  = Path(os.getenv("DATA_RAW_DIR", "/opt/data/raw"))
BUCKET   = "bronze"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"Bucket '{BUCKET}' already exists.")
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Created bucket '{BUCKET}'.")

# (src_glob_dir, glob_pattern, s3_prefix)
UPLOADS = [
    (RAW_DIR / "job_events",     "job_events_*.jsonl",        "job_events"),
    (RAW_DIR / "job_events",     "job_completions_*.jsonl",   "job_completions"),
    (RAW_DIR / "inference_logs", "inference_logs_*.jsonl",    "inference_logs"),
]

total = 0
for src_dir, pattern, prefix in UPLOADS:
    files = sorted(src_dir.glob(pattern))
    if not files:
        print(f"  WARNING: no files matching {src_dir}/{pattern}")
        continue
    for f in files:
        key = f"{prefix}/{f.name}"
        s3.upload_file(str(f), BUCKET, key)
        size_mb = f.stat().st_size / 1_048_576
        print(f"  {f.name} ({size_mb:.1f} MB) → s3a://{BUCKET}/{key}")
        total += 1

print(f"\nDone. {total} file(s) uploaded to bucket '{BUCKET}'.")
if total == 0:
    print("No files uploaded — did you run 'make generate' first?")
    sys.exit(1)
