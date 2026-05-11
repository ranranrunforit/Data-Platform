# Security

What the platform implements today, what it does not, and what would be added before production.

This is a **development-grade** platform optimised for local Docker Compose. Several defaults that are convenient for development (default passwords, no TLS, open ports) would be unacceptable in production. This document is honest about the gap.

---

## What is implemented

### Secrets management

- `.env.example` is committed; `.env` is git-ignored.
- `make up` auto-generates two cryptographic secrets if `.env` still has placeholders:
  - **Airflow Fernet key** — encrypts connection passwords stored in the metadata DB (`AIRFLOW__CORE__FERNET_KEY`)
  - **Airflow webserver secret** — signs session cookies (`AIRFLOW__WEBSERVER__SECRET_KEY`)
- Generation is one-shot and idempotent; re-running `make up` does not regenerate keys, preserving Airflow connections.

### Service authentication

- **Airflow webserver** — username/password (default `admin`/`admin`); change via `AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD` in `.env`.
- **MinIO console** — root user/password (default `minioadmin`/`minioadmin123`); change via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`.
- **Postgres** — Airflow service account; password via `POSTGRES_PASSWORD`.
- **Redis** — optional password via `REDIS_PASSWORD`. Empty by default; broker URL in compose is `redis://:${REDIS_PASSWORD:-}@redis:6379/0`.

The `.env.example` file explicitly documents that an empty `REDIS_PASSWORD` is acceptable only on a private Docker network.

### Network isolation

- All inter-service traffic stays on the default Docker Compose bridge network.
- Only the UI ports listed in [Services](../README.md#services) are exposed to the host.
- Spark master and Kafka brokers expose host ports (7077, 9092/9093/9094) for development convenience — these would not be exposed in production.

### Kafka durability

- 3 brokers, `default.replication.factor=3`, `min.insync.replicas=2`, `acks=all` on the durability-critical job-events producer.
- Single-broker failure causes neither data loss nor producer errors.
- The inference producer uses `acks=1` (leader-only) deliberately — inference logs are tolerant to occasional loss in exchange for higher throughput.

### Delta Lake ACID

- Silver and Gold writes are transactional. Concurrent writers serialise via the Delta transaction log; readers see consistent snapshots.
- 7-day time-travel retention provides a recovery window for accidental writes (`VACUUM RETAIN 168 HOURS` runs nightly).

### CI / supply chain

- GitHub Actions pins exact tool versions: `ruff==0.4.4`, `pyspark==3.5.1`, `delta-spark==3.1.0`, `pytest==8.1.1`, `dbt-duckdb==1.7.4`, `great-expectations==0.18.19`.
- Container base images are versioned: `apache/airflow:2.10.4-python3.11`, `apache/spark:3.5.4`, `confluentinc/cp-kafka:7.5.3`, `postgres:15-alpine`, `redis:7.2-alpine`.

---

## What is not implemented (and what to add for production)

### Authentication & authorization

| Surface | Today | Production |
|---|---|---|
| FastAPI `/cost`, `/sla`, `/utilization` | No auth | OAuth2 / OIDC via FastAPI dependencies; per-org RBAC at the query layer |
| MinIO root user | Plain key/secret in `.env` | IAM roles + temporary credentials (STS); rotate keys |
| Airflow | Single admin user | SSO (Google / Okta / Azure AD) via `auth_backend`; per-DAG RBAC |
| Postgres | Single Airflow account | Separate read-only role for analytics; managed RDS with IAM auth |
| Kafka | Plaintext, no SASL | SASL_SSL with mTLS or SCRAM; per-topic ACLs |

### Encryption

| Layer | Today | Production |
|---|---|---|
| Object storage at rest | MinIO unencrypted | S3 SSE-KMS / SSE-S3, or MinIO with KMS sidecar |
| Object storage in transit | `s3a://` plaintext over Docker bridge | HTTPS to S3; `fs.s3a.connection.ssl.enabled=true` |
| Kafka | PLAINTEXT listeners | SASL_SSL on a separate listener; ACL per service |
| Postgres | Plaintext on Docker bridge | TLS-required connections; AWS RDS in a private subnet |
| Airflow webserver | HTTP on :8081 | HTTPS via reverse proxy (nginx / ALB) with proper certs |
| API | HTTP on :8000 | HTTPS via reverse proxy; HSTS |

### Secrets management

The `.env` file works for one-machine development. Production should source secrets from a real provider:

- AWS Secrets Manager / Parameter Store with IAM role access from EC2 / ECS / EKS
- HashiCorp Vault with auto-rotation
- Sealed Secrets / External Secrets Operator on Kubernetes
- Airflow's own [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html) interface (Vault / AWS / GCP backends supported out-of-the-box)

The `airflow-init` container currently writes the `aws_default` connection from env vars at startup. Replace with a Secrets Backend so Airflow fetches credentials per task at runtime.

### Audit logging

- No central audit log of API queries. FastAPI access logs go to stdout and are captured by Docker's json-file driver, but there is no aggregation.
- Airflow logs every task execution to `/opt/airflow/logs` (a local volume). Production should ship to S3 / CloudWatch / Stackdriver via `AIRFLOW__LOGGING__REMOTE_LOGGING`.
- DuckDB queries do not log who issued them — the API would need to wrap each handler with a structured logger that captures requester identity once auth is added.

### Network model

- All services live on a flat bridge network. Production should:
  - Run application services in a private subnet
  - Front the API and Airflow with a VPN or zero-trust proxy (Cloudflare Access, Tailscale, IAP)
  - Restrict MinIO / S3 to VPC endpoint access only
  - Place Kafka and Spark on a separate data-plane subnet with security-group rules limiting ingress to known producers / consumers

### Container hardening

- API (`serving/Dockerfile`) and Airflow (`Dockerfile.airflow`) run as non-root users (`airflow`, default Python `nobody`-style for the API base).
- Spark worker runs as the `spark` user from the upstream image.
- No `USER 0` instructions in our Dockerfiles after package install.

What is missing:
- No image scanning in CI (Trivy / Grype)
- No `--read-only` filesystem mounts
- No seccomp / AppArmor profiles
- No SBOM generation

### Dependency vulnerability scanning

Dependabot or Renovate would catch CVEs in:
- `requirements-airflow.txt` (Airflow + pinned providers)
- `serving/requirements.txt` (FastAPI / DuckDB / boto3)
- The Spark `--packages` Maven coordinates (Delta Lake, hadoop-aws)
- Container base images

None of this is wired up today.

---

## Threat model

For the local Docker Compose deployment, the trust boundary is the host machine. An attacker with shell access to the host can:

- Read `.env` (all credentials)
- Exec into any container (`docker exec`)
- Read the contents of `minio-data`, `postgres-data`, `kafka-*-data` volumes
- Forge API and Kafka traffic on the bridge network

This is acceptable for local development because the same attacker would have full host access regardless. **Do not expose any of the published ports to a public network.**

For production, the threat model expands to:
- External users hitting the API → mitigated by OAuth2 + per-org RBAC + rate limiting
- Internal users of one org seeing another org's data → mitigated by query-layer filtering + per-org IAM roles
- Compromised CI runner pushing malicious images → mitigated by signed images (Sigstore) + admission control
- Insider with read access to S3 → mitigated by SSE-KMS with per-bucket keys + CloudTrail

---

## Known dev-only conveniences (must change before prod)

| File | Setting | Why it's there | What to change |
|---|---|---|---|
| `.env.example` | `AIRFLOW_ADMIN_PASSWORD=admin` | Demo convenience | Source from secrets backend |
| `.env.example` | `AWS_SECRET_ACCESS_KEY=minioadmin123` | MinIO root credentials | Use IAM role, not static keys |
| `docker-compose.yml` | Postgres / Redis on Docker bridge | One-command bring-up | Move to managed services in private subnet |
| `dbt_project.yml` | `CREATE OR REPLACE PERSISTENT SECRET minio` with hard-coded creds | dbt-duckdb 1.7.4 ignores `secrets:` block; `delta_kernel-rs` ignores `SET s3_*` | Render via Jinja from env vars before deploy |
| `serving/main.py` | `CORSMiddleware allow_origins=["*"]` | Demo browser access | Restrict to known UI origins |
| `serving/main.py` | `:memory:` DuckDB with embedded creds in `SET s3_*` | Self-contained API | Move to mounted IAM role / IRSA |
| `kafka-init` | Plain `kafka-topics --create` | Bootstrap convenience | Provision via Terraform with ACLs |

Each of these is a deliberate trade-off documented in source. The same pattern appears in [GOVERNANCE.md](GOVERNANCE.md): development conveniences are explicit, with a clear migration path to a hardened production setup.
