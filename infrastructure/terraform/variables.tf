variable "minio_endpoint" {
  description = "MinIO server endpoint (no scheme)"
  type        = string
  default     = "localhost:9000"
}

variable "minio_access_key" {
  description = "MinIO root user / AWS access key"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "minio_secret_key" {
  description = "MinIO root password / AWS secret key"
  type        = string
  default     = "minioadmin123"
  sensitive   = true
}
