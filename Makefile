.PHONY: help up down reset scale-workers generate ingest pipeline \
        dbt-run dbt-test gx-validate stream-start test lint ci \
        kafka-topics kafka-lag logs ps

SHELL := /bin/bash
COMPOSE := docker compose
BASE_DIR := $(shell pwd)

help:  ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ─────────────────────────────────────────────────────────────

up: ## Start all services  (first run ~8 min — downloads Spark + Kafka images)
	@cp -n .env.example .env 2>/dev/null || true
	@# Generate Fernet key if still placeholder
	@if grep -q "REPLACE_WITH_GENERATED_KEY" .env; then \
		FERNET=$$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"); \
		sed -i "s|REPLACE_WITH_GENERATED_KEY|$$FERNET|g" .env; \
		echo "  ✓ Generated Airflow Fernet key"; \
	fi
	@# Generate webserver secret if still placeholder
	@if grep -q "REPLACE_WITH_GENERATED_SECRET" .env; then \
		SECRET=$$(python3 -c "import secrets; print(secrets.token_hex(32))"); \
		sed -i "s|REPLACE_WITH_GENERATED_SECRET|$$SECRET|g" .env; \
		echo "  ✓ Generated Airflow webserver secret"; \
	fi
	$(COMPOSE) up -d --build --scale airflow-worker=2 --scale spark-worker=2
	@echo ""
	@echo "  Services starting... wait ~60s for Airflow to initialise."
	@echo ""
	@echo "  Airflow UI   → http://localhost:8081  (admin / admin)"
	@echo "  Flower UI    → http://localhost:5555  (Celery workers)"
	@echo "  Spark UI     → http://localhost:8080"
	@echo "  Kafka UI     → http://localhost:8082"
	@echo "  MinIO UI     → http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  API docs     → http://localhost:8000/docs"
	@echo ""
	@echo "  Next: make generate && make ingest && make pipeline"

down: ## Stop all services (preserves volumes)
	$(COMPOSE) down

reset: ## Destroy everything including volumes — DESTRUCTIVE
	@echo "WARNING: This will delete all data. Ctrl+C to cancel."
	@sleep 3
	$(COMPOSE) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail logs for all services
	$(COMPOSE) logs -f

logs-%: ## Tail logs for a specific service (e.g. make logs-airflow-scheduler)
	$(COMPOSE) logs -f $*

# ── Scaling ────────────────────────────────────────────────────────────────────

scale-workers: ## Scale Airflow Celery workers (usage: make scale-workers N=4)
	$(COMPOSE) up -d --scale airflow-worker=$(or $(N),2)
	@echo "Celery workers scaled to $(or $(N),2)"

scale-spark: ## Scale Spark workers (usage: make scale-spark N=4)
	$(COMPOSE) up -d --scale spark-worker=$(or $(N),2)
	@echo "Spark workers scaled to $(or $(N),2)"

# ── Data generation + ingestion ────────────────────────────────────────────────

generate: ## Generate synthetic AI-infra telemetry → data/raw/
	@echo "Generating synthetic data..."
	python3 data/generator/job_events.py
	python3 data/generator/inference_logs.py
	python3 data/generator/node_metrics.py
	@echo "Done. Files in data/raw/"

ingest: upload-bronze ## Publish historical data to Kafka and upload raw files to MinIO bronze
	@echo "Publishing job events..."
	$(COMPOSE) run --rm airflow-worker \
		python /opt/ingestion/kafka/producers/job_producer.py
	@echo "Publishing inference logs..."
	$(COMPOSE) run --rm airflow-worker \
		python /opt/ingestion/kafka/producers/inference_producer.py

upload-bronze: ## Upload raw JSONL files from data/raw/ to MinIO bronze bucket
	@echo "Uploading raw data to MinIO bronze..."
	$(COMPOSE) run --rm airflow-worker \
		python /opt/ingestion/upload_to_bronze.py

# ── Pipeline execution ─────────────────────────────────────────────────────────

pipeline: ## Run full batch pipeline (bronze → silver → GX → gold)
	@echo "==> Step 1: Spark bronze → silver"
	@echo "Staging Spark jobs into container..."
	docker exec spark-master mkdir -p /tmp/spark-apps/jobs /tmp/spark-apps/utils
	docker cp spark/jobs/. spark-master:/tmp/spark-apps/jobs/
	docker cp spark/utils/. spark-master:/tmp/spark-apps/utils/
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master local[2] \
		--driver-memory 1500m \
		--packages $(shell grep SPARK_PACKAGES .env | cut -d= -f2) \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--conf spark.eventLog.enabled=false \
		--conf spark.sql.shuffle.partitions=4 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		/tmp/spark-apps/jobs/bronze_to_silver.py
	@echo "==> Step 2: GX quality gate"
	$(MAKE) gx-validate
	@echo "==> Step 3: dbt silver → gold"
	$(MAKE) dbt-run
	@echo "Pipeline complete."

dbt-run: ## Run dbt models
	$(COMPOSE) exec airflow-worker \
		dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod

dbt-test: ## Run dbt tests
	$(COMPOSE) exec airflow-worker \
		dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt --target prod

gx-validate: ## Run Great Expectations checkpoint against silver layer
	$(COMPOSE) exec airflow-worker \
		python /opt/quality/checkpoints/silver_checkpoint.py

stream-start: ## Start Spark Structured Streaming consumer (Ctrl+C to stop)
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
		--master local[2] \
		--driver-memory 1500m \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--conf spark.eventLog.enabled=false \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		/opt/spark-apps/jobs/streaming_consumer.py

stream-live: ## Start live inference producer (100 req/s → Kafka)
	$(COMPOSE) run --rm airflow-worker \
		python /opt/ingestion/kafka/producers/inference_producer.py --live --rate 100

# ── Kafka operations ───────────────────────────────────────────────────────────

kafka-topics: ## List Kafka topics
	$(COMPOSE) exec kafka-1 kafka-topics \
		--bootstrap-server kafka-1:29092 --list

kafka-lag: ## Show consumer group lag
	$(COMPOSE) exec kafka-1 kafka-consumer-groups \
		--bootstrap-server kafka-1:29092 --all-groups --describe

kafka-offsets: ## Show topic offsets
	$(COMPOSE) exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
		--broker-list kafka-1:29092 --topic inference-api-logs

# ── Quality + Tests ────────────────────────────────────────────────────────────

test: ## Run unit tests (PySpark local mode)
	pytest tests/unit/ -v --tb=short

lint: ## Lint Python with ruff
	ruff check spark/ ingestion/ data/ serving/ quality/ orchestration/ tests/

# ── CI ─────────────────────────────────────────────────────────────────────────

ci: lint test dbt-test gx-validate ## Run full local CI suite
