variable "aws_region" {
  description = "AWS region to deploy infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key" {
  description = "AWS Access Key"
  type        = string
}

variable "aws_secret_key" {
  description = "AWS Secret Key"
  type        = string
}

variable "s3_raw_bucket_name" {
  description = "Name of the S3 bucket for raw Kafka data"
  default     = "citysim-raw-data-bucket"
}

variable "s3_processed_bucket_name" {
  description = "Name of the S3 bucket for processed analytics data"
  default     = "citysim-processed-data-bucket"
}

variable "kafka_cluster_name" {
  description = "MSK (Managed Kafka) Cluster Name"
  type        = string
  default     = "citysim-kafka-cluster"
}
