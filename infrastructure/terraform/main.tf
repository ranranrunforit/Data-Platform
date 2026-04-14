/**
 * Terraform: MinIO bucket provisioning for local development.
 *
 * Provisions the three-layer medallion storage structure:
 *   bronze/  → raw ingested data (append-only)
 *   silver/  → cleaned + enriched Delta tables
 *   gold/    → dbt Gold models (business-ready)
 *
 * Also provisions:
 *   checkpoints/ → Spark Structured Streaming checkpoint location
 *
 * Why Terraform even for local dev:
 *   - Same IaC code works against real S3 by changing the provider config
 *   - Bucket policies and lifecycle rules are version-controlled
 *   - Shows operational maturity (ADR-003 explains this decision)
 *
 * Usage:
 *   terraform init
 *   terraform plan
 *   terraform apply
 */

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = "~> 2.0"
    }
  }
}

provider "minio" {
  minio_server   = var.minio_endpoint
  minio_user     = var.minio_access_key
  minio_password = var.minio_secret_key
  minio_ssl      = false
}

# ── Bronze bucket ─────────────────────────────────────────────────────────────
resource "minio_s3_bucket" "bronze" {
  bucket = "bronze"
  acl    = "private"
}

resource "minio_s3_bucket_versioning" "bronze" {
  bucket = minio_s3_bucket.bronze.bucket

  versioning_configuration {
    status = "Suspended"   # no versioning on raw data — rely on Delta log
  }
}

# ── Silver bucket ─────────────────────────────────────────────────────────────
resource "minio_s3_bucket" "silver" {
  bucket = "silver"
  acl    = "private"
}

# ── Gold bucket ───────────────────────────────────────────────────────────────
resource "minio_s3_bucket" "gold" {
  bucket = "gold"
  acl    = "private"
}

# ── Checkpoints bucket ────────────────────────────────────────────────────────
resource "minio_s3_bucket" "checkpoints" {
  bucket = "checkpoints"
  acl    = "private"
}

# ── Notification: print bucket names after apply ─────────────────────────────
output "buckets_created" {
  value = {
    bronze      = minio_s3_bucket.bronze.bucket
    silver      = minio_s3_bucket.silver.bucket
    gold        = minio_s3_bucket.gold.bucket
    checkpoints = minio_s3_bucket.checkpoints.bucket
  }
}
