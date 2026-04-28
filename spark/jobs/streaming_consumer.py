"""
Spark Structured Streaming: Kafka → Delta Lake (Bronze).

Consumes inference API logs from the 'inference-api-logs' Kafka topic
and writes them to the Delta bronze layer with micro-batch processing.

Why Structured Streaming here vs batch:
  - Inference requests need real-time SLO monitoring (p99 latency dashboards)
  - Batch would have 24h lag; streaming gives 30-second lag
  - Same Delta table is later read by the daily batch Airflow DAG — one table, two consumers

Exactly-once semantics are guaranteed by:
  - Kafka offset checkpointing (HDFS/S3-compatible checkpoint location)
  - Delta Lake's idempotent writes + transaction log

Run:
  make stream-start
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/opt/spark-apps")
from utils.delta_utils import get_spark_session

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:29092,kafka-2:29093,kafka-3:29094",
)
KAFKA_TOPIC = "inference-api-logs"
BRONZE_PATH = os.getenv("DATA_BRONZE_PATH", "s3a://bronze")
CHECKPOINT_PATH = os.getenv("DATA_CHECKPOINT_PATH", "s3a://checkpoints")

# ── Schema of the Kafka value payload (matches inference_producer) ─────────
INFERENCE_SCHEMA = StructType([
    StructField("request_id",    StringType(),    False),
    StructField("org_id",        StringType(),    True),
    StructField("user_id",       StringType(),    True),
    StructField("model_id",      StringType(),    True),
    StructField("region",        StringType(),    True),
    StructField("status_code",   IntegerType(),   True),
    StructField("cache_hit",     BooleanType(),   True),
    StructField("input_tokens",  IntegerType(),   True),
    StructField("output_tokens", IntegerType(),   True),
    StructField("total_tokens",  IntegerType(),   True),
    StructField("latency_ms",    DoubleType(),    True),
    StructField("cost_usd",      DoubleType(),    True),
    StructField("timestamp",     TimestampType(), True),
])


def main():
    spark = get_spark_session("streaming_inference_consumer")

    # ── Read from Kafka ───────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10_000)   # back-pressure: cap per micro-batch
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),
                INFERENCE_SCHEMA,
            ).alias("data"),
        )
        .select(
            "kafka_offset",
            "kafka_partition",
            "kafka_timestamp",
            "data.*",
        )
        # Watermark: allow up to 10 minutes of late data before closing a window
        .withWatermark("timestamp", "10 minutes")
        .withColumn("log_date", F.to_date("timestamp"))
        .withColumn("log_hour", F.hour("timestamp"))
        .withColumn("_stream_ingested_at", F.current_timestamp())
    )

    # ── Write to Delta (bronze/inference_stream) ──────────────────────────────
    # Append-only: streaming data is never updated.
    # The daily batch job reads this table for Silver transforms.
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation",
                f"{CHECKPOINT_PATH}/inference_stream")
        .option("path", f"{BRONZE_PATH}/inference_stream")
        .partitionBy("log_date")
        .trigger(processingTime="30 seconds")   # micro-batch interval
        .start()
    )

    print(f"Streaming consumer started. Topic: {KAFKA_TOPIC}")
    print(f"Writing to: {BRONZE_PATH}/inference_stream")
    print(f"Checkpoint: {CHECKPOINT_PATH}/inference_stream")
    print("Awaiting termination (Ctrl+C to stop)...")

    query.awaitTermination()


if __name__ == "__main__":
    main()
