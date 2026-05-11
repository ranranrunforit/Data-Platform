# ADR-004: Terraform for Infrastructure as Code

**Status:** Accepted  
**Date:** 2026-01-17  
**Deciders:** Data Engineering

---

## Context

The platform needs four object-storage buckets (`bronze`, `silver`, `gold`, `checkpoints`) with consistent configuration: ACL, optional versioning, lifecycle expiry on Bronze, and identical bucket layout in dev and prod.

Three places could create them:

1. Click-ops in the MinIO / S3 console ‚Ä?fastest first time, drift-prone, no audit trail.
2. A shell script that runs `mc mb` / `aws s3 mb` ‚Ä?captures intent in code but no state tracking, no plan-before-apply, no diff against actual.
3. Terraform ‚Ä?full state, plan/apply, provider-portable.

The same consideration applies in production for IAM roles, S3 bucket policies, lifecycle rules, MSK clusters, RDS instances, etc. If those eventually go to Terraform, the bucket provisioning should too ‚Ä?there should be one IaC story, not two.

## Options considered

| Tool | Multi-cloud | State tracking | Plan/apply | MinIO support | Notes |
|---|---|---|---|---|---|
| **Terraform** | ‚ú?| ‚ú?| ‚ú?| ‚ú?via `aminueza/minio` provider | Industry standard |
| Pulumi | ‚ú?| ‚ú?| ‚ú?| ‚ö†Ô∏è via provider package | Programming-language-based; smaller community |
| AWS CDK | ‚ù?AWS-only | ‚ú?| ‚ú?| ‚ù?| Cloud-locked |
| `mc ilm` / `aws s3` shell scripts | n/a | ‚ù?| ‚ù?| ‚ú?| Used inside `minio-init` for one-time setup, not as IaC |
| Click-ops | n/a | ‚ù?| ‚ù?| ‚ú?| Drift-prone |

## Decision

**Terraform with the `aminueza/minio` provider in development; same `.tf` files port to AWS S3 by swapping the provider block.**

### Why Terraform over Pulumi

Both work. Terraform wins on hiring-pool size and ecosystem breadth ‚Ä?the `hashicorp/aws` provider covers every service the platform might ever use (MSK, RDS, IAM, EKS, EMR), and most prospective contributors already know HCL.

### Why Terraform alongside `mc` / `kafka-topics` scripts

The `minio-init` and `kafka-init` containers in Compose still apply some configuration imperatively (lifecycle rules on Bronze, topic creation with RF=3). They run on every `make up` and are idempotent. Two reasons not to fold them into Terraform:

- They depend on the services being up; Terraform is for the provisioning step before services know about each other.
- They are bound to the dev compose file and would not run in production at all (production uses MSK / managed S3 with their own provisioning paths).

Terraform handles **the durable resources** (buckets, future MSK clusters, IAM); the init containers handle **the runtime configuration** that fits naturally in compose.

## How it is used

```bash
cd infrastructure/terraform
terraform init
terraform plan       # diff against current MinIO state
terraform apply      # creates 4 buckets
```

Source: [infrastructure/terraform/main.tf](../../../infrastructure/terraform/main.tf), [variables.tf](../../../infrastructure/terraform/variables.tf).

The provider block:

```hcl
provider "minio" {
  minio_server   = var.minio_endpoint
  minio_user     = var.minio_access_key
  minio_password = var.minio_secret_key
  minio_ssl      = false
}
```

Each bucket is a `minio_s3_bucket` resource with `acl = "private"`. The Bronze bucket additionally has a `minio_s3_bucket_versioning` block (status `Suspended` ‚Ä?Delta's transaction log handles versioning on Silver / Gold, and Bronze is reproducible).

To target real AWS S3, swap the provider:

```hcl
provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "ai-platform-bronze"
}
```

Resource shape is the same; the rest of the config stays.

## Consequences

- Bucket layout is reproducible from a fresh laptop ‚Ä?no manual setup steps are required after `terraform apply`.
- The same Terraform code is the migration plan for production: change provider, change variables, `apply`.
- State is currently kept locally (default backend). Production should configure an S3 + DynamoDB backend for shared state with locking.
- Sensitive variables (`minio_access_key`, `minio_secret_key`) are marked `sensitive = true` so they are masked in plan output. Production sources them from AWS Secrets Manager / Vault rather than `terraform.tfvars`.
- Adding new resources (Kafka topics in MSK, IAM roles, RDS instances) follows the same pattern ‚Ä?declarative, version-controlled, plan-before-apply.
