# ADR-005: Docker Compose for Development, Kubernetes for Production

**Status:** Accepted  
**Date:** 2026-01-18  
**Deciders:** Data Engineering  

---

## Context

The platform has 13 long-running services (MinIO, Redis, 3 Kafka brokers, Zookeeper, Postgres, Spark master + 2 workers, Airflow init/scheduler/webserver/2 workers/triggerer/flower, Kafka UI, FastAPI). They have non-trivial startup ordering: Postgres before Airflow init, Zookeeper before Kafka, Kafka before kafka-init, MinIO before its buckets, Airflow init before scheduler/workers/webserver.

Two distinct lifecycles need to be supported:

- **Development / demo / CI** - one-command "everything up" on a laptop or Codespace, repeatable, isolated, low-friction tear-down.
- **Production** - autoscaling, secrets from a real backend, multi-zone resilience, managed databases / brokers, image rollback, blue-green deployments.

Trying to use one tool for both ends in compromise: Compose has no autoscaling and no secrets backend; Kubernetes is overkill on a laptop and adds 30 seconds of YAML to every "let me try one thing" iteration.

## Options considered

For development:

| Tool | Startup time | Multi-service | Health checks | Resource limits | Learning curve |
|---|---|---|---|---|---|
| **Docker Compose** | ~60 s warm | - | - | - via `deploy.resources` | Low |
| Kind / Minikube + Helm | ~3 min cold | - | - | - | Medium |
| Tilt / Skaffold | ~2 min | - | - | - | Medium |
| Bare `docker run` scripts | Fast per service | - no graph | Manual | Manual | Low |

For production:

| Target | Autoscaling | Managed services integration | Spark / Airflow story | Notes |
|---|---|---|---|---|
| **Kubernetes (EKS / GKE / AKS)** | - | - via IRSA / Workload Identity | spark-on-k8s-operator, KubernetesExecutor, Airflow Helm chart | Industry standard |
| ECS / Fargate | - | - AWS-only | ⚠️ no first-class Spark; needs EMR Serverless | AWS-locked |
| Nomad | - | ⚠️ less rich | ⚠️ custom | Smaller ecosystem |
| EC2 + systemd | ⚠️ manual | ⚠️ | ⚠️ manual | Pre-Docker era |

## Decision

**Docker Compose for development; Kubernetes (specifically EKS / GKE / AKS) as the documented production target.**

### Why Compose for development

- Single declarative file ([docker-compose.yml](../../../docker-compose.yml)) describes every service, network, volume, healthcheck, and resource limit.
- `depends_on: { condition: service_healthy }` enforces the startup order without separate scripting.
- `--scale airflow-worker=N` and `--scale spark-worker=N` give horizontal scaling without touching the compose file. Wired into [Makefile](../../../Makefile) as `make scale-workers N=4` / `make scale-spark N=4`.
- `make up` covers the whole bring-up including auto-generation of Airflow Fernet keys.
- Codespaces and Oracle Cloud Free Tier both run Compose natively - same setup commands across local, demo, and a free always-on environment.
- CI integration test in [.github/workflows/ci.yml](../../../.github/workflows/ci.yml) brings up a subset (`minio + zookeeper + kafka-1`) on every push to main.

### Why Kubernetes for production (and not Compose at scale)

Compose has no autoscaling, no node placement, no rolling deployments, no managed-secrets integration, no multi-AZ scheduling. Production needs all of these. Kubernetes is the only target that gives the platform:

- **Autoscaling** for both Airflow workers (KEDA on Celery queue length) and Spark executors (spark-on-k8s-operator dynamic allocation).
- **Per-pod resource isolation** for Spark jobs - every job becomes a pod with its own CPU/memory request, so a runaway transform cannot evict the scheduler.
- **Managed-secret integration** via External Secrets Operator pulling from AWS Secrets Manager / GCP Secret Manager / Vault.
- **Rolling deployments** for the API and Airflow without pipeline downtime.
- **Multi-AZ scheduling** for stateless services; multi-AZ replication for managed dependencies (RDS / MSK / S3).

### Why containerise at all

The same images that run on a laptop run in CI run on Kubernetes. Spark + Airflow + dbt + DuckDB + Java together have a non-trivial install footprint ([Dockerfile.airflow](../../../Dockerfile.airflow) is 25 lines but pulls in JDK 17 and Spark 3.5.1); containers freeze that footprint and remove "works on my machine" entirely.

## How it is used

### Docker Compose (today)

- [docker-compose.yml](../../../docker-compose.yml) - 646 lines covering 14 services, 3 custom networks (default bridge), 9 named volumes, healthchecks on every long-running service, resource limits via `deploy.resources`.
- [Dockerfile.airflow](../../../Dockerfile.airflow) - `apache/airflow:2.10.4-python3.11` + JDK 17 + Spark 3.5.1 client + pinned `requirements-airflow.txt`.
- [Dockerfile.spark](../../../Dockerfile.spark) - `apache/spark:3.5.4` + `delta-spark==3.1.0`.
- [serving/Dockerfile](../../../serving/Dockerfile) - `python:3.11-slim` + FastAPI + DuckDB + boto3.
- [Makefile](../../../Makefile) wraps every common operation. `make up`, `make scale-workers`, `make logs-<service>`, `make down`, `make reset`.

### Kubernetes (production migration path)

Documented in [DEPLOYMENT.md § Production (cloud-native)](../../operations/DEPLOYMENT.md#production-cloud-native):

- **Airflow** - official Helm chart (`apache-airflow/airflow`) with `KubernetesExecutor` instead of CeleryExecutor. Each task spawns a pod with its own resource request - no Celery worker pool to size.
- **Spark** - spark-on-k8s-operator. `SparkSubmitOperator` becomes `SparkKubernetesOperator`. The PySpark job code in [spark/jobs/](../../../spark/jobs/) does not change.
- **Kafka** - AWS MSK or Confluent Cloud (managed); not run inside K8s.
- **Postgres / Redis** - RDS / ElastiCache (managed); not run inside K8s.
- **MinIO** - AWS S3 (managed); same `s3a://` URLs, same code.
- **API** - standard K8s `Deployment` + `Service` + Ingress, or Cloud Run / ECS Fargate for lower ops overhead.

The same container images work in both targets. The migration is a configuration change (Compose - Helm values), not a re-architecture.

## Consequences

- One stack definition (`docker-compose.yml`) is the source of truth for development; equivalent Helm charts (or Terraform modules wrapping them) take over for production.
- The Compose stack will not be run in production - its postgres/redis/kafka services exist only because there is no managed equivalent on a laptop.
- The CI integration test exercises the Compose path; production deployment needs its own E2E test suite against a staging K8s cluster.
- Kubernetes is documented but not implemented in this repo. Adding K8s manifests / a Helm chart is a follow-up that would not change any application code - only Operator-equivalent wiring.
- Resource limits in `docker-compose.yml` (e.g. `airflow-worker: 4 GB memory`) are tuned for a 16 GB laptop. Production Helm values will be much higher.
