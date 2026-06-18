variable "name_prefix" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "iceberg_bucket_name" {
  type = string
}

variable "checkpoints_bucket_name" {
  type = string
}

variable "artifacts_bucket_name" {
  type = string
}

variable "spark_irsa_role_arn" {
  type = string
}

variable "spark_operator_image_repository" {
  description = "ECR repo for the custom spark-operator image (hadoop-aws baked in)"
  type        = string
}

variable "spark_operator_image_tag" {
  type    = string
  default = "v1beta2-1.4.6-3.5.0"
}

variable "airflow_irsa_role_arn" {
  type = string
}

variable "cluster_autoscaler_irsa_role_arn" {
  type = string
}

variable "cluster_name" {
  description = "EKS cluster name — used by cluster-autoscaler auto-discovery"
  type        = string
}

variable "dags_git_repo" {
  type = string
}

variable "dags_git_branch" {
  type    = string
  default = "main"
}

variable "dags_git_subpath" {
  type    = string
  default = "airflow/dags"
}

variable "kafka_cluster_name" {
  type    = string
  default = "logistics-kafka"
}

variable "kafka_version" {
  description = "Kafka version deployed by Strimzi"
  type        = string
  default     = "3.7.0"
}

variable "kafka_storage_size" {
  description = "PVC size for Kafka broker storage"
  type        = string
  default     = "50Gi"
}

variable "clickhouse_db" {
  type    = string
  default = "logistics"
}

variable "clickhouse_username" {
  type    = string
  default = "admin"
}
