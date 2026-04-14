# Architecture: AI Infrastructure Data Platform

## System Overview

```mermaid
flowchart TD
    subgraph Sources["Data Sources"]
        GEN["Synthetic Generator\n(GPU jobs · inference logs\nnode metrics)"]
    end

    subgraph Ingestion["Ingestion Layer"]
        KP["Kafka Producers\njob_producer.py\ninference_producer.py"]
        KC["Kafka\ngpu-job-events\ninference-api-logs"]
        BATCH["Batch CSV Drop\n(node_metrics)"]
    end

    subgraph Bronze["☁️ Bronze — MinIO s3://bronze"]
        B1["job_events/\n(Delta)"]
        B2["inference_stream/\n(Delta, micro-batch 30s)"]
        B3["node_metrics/\n(Parquet CSV)"]
    end

    subgraph Silver["🥈 Silver — MinIO s3://silver"]
        S1["jobs/\n(Delta, partitioned by job_date)\nMERGE: late completions"]
        S2["inference/\n(Delta, partitioned by log_date)"]
        S3["node_metrics/\n(Delta, hourly)"]
    end

    subgraph Quality["Quality Gate"]
        GX["Great Expectations\nsilver_checkpoint.py\nRow counts · null checks\nvalue distributions"]
    end

    subgraph Gold["🥇 Gold — dbt + DuckDB"]
        G1["cost_attribution\n(billing mart)"]
        G2["gpu_utilization_hourly\n(capacity planning)"]
        G3["job_performance_sla\n(SLO monitoring)"]
    end

    subgraph Orchestration["Orchestration — Airflow"]
        DAG1["batch_pipeline_daily\n(01:00 UTC)"]
        DAG2["streaming_health_check\n(every 15 min)"]
    end

    subgraph Serving["Serving — FastAPI + DuckDB"]
        API["REST API :8000\n/cost/orgs\n/utilization/hourly\n/sla/models"]
    end

    subgraph Streaming["Spark Structured Streaming"]
        SS["streaming_consumer.py\nKafka → Delta\n30s micro-batches\nexactly-once"]
    end

    GEN --> KP --> KC
    GEN --> BATCH --> B3

    KC -->|"Spark bronze_to_silver.py\n(batch)"| B1
    KC -->|SS| B2
    BATCH --> B3

    B1 --> S1
    B2 --> S2
    B3 --> S3

    S1 --> GX
    GX -->|"pass → promote"| G1
    GX -->|"fail → block"| DAG1

    S1 --> G1
    S2 --> G3
    S3 --> G2

    G1 --> API
    G2 --> API
    G3 --> API

    DAG1 -->|"orchestrates"| S1
    DAG2 -->|"monitors"| G3
```

## Storage Layout

```
MinIO
├── bronze/
│   ├── job_events/          ← raw JSON (append-only)
│   ├── job_completions/     ← late-arriving completion events
│   ├── inference_stream/    ← streaming micro-batches (30s)
│   └── node_metrics/        ← CSV batch drops
│
├── silver/                  ← Delta Lake (ACID, MERGE)
│   ├── jobs/                ← partitioned by job_date
│   │   └── _delta_log/
│   ├── inference/           ← partitioned by log_date
│   └── node_metrics/        ← partitioned by metric_date
│
├── gold/                    ← dbt output (Delta via DuckDB)
│   ├── cost_attribution/
│   ├── gpu_utilization_hourly/
│   └── job_performance_sla/
│
└── checkpoints/             ← Spark Structured Streaming offsets
    └── inference_stream/
```

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Table format | Delta Lake | ACID + MERGE for late-arriving events; industry standard |
| Batch engine | Apache Spark (PySpark) | Distributed, handles TB-scale; JD requirement |
| Streaming | Spark Structured Streaming + Kafka | Same engine as batch; exactly-once via Delta + offsets |
| Transforms | dbt + DuckDB | SQL-based, testable, version-controlled; DuckDB reads Delta natively |
| Quality | Great Expectations | Declarative assertions; fails pipeline, not just warns |
| Orchestration | Airflow | Sensor-based, wide ecosystem, matches JD requirement |
| Local storage | MinIO | S3-compatible API; same boto3/s3a code runs in AWS |
| IaC | Terraform | Bucket policies version-controlled; shows operational maturity |

See ADRs for deeper reasoning: [ADR-001](adr/001-delta-lake-vs-parquet.md) · [ADR-002](adr/002-airflow-vs-prefect.md)
