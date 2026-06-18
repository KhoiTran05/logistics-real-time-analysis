variable "name_prefix" {
  type = string
}

variable "account_id" {
  type = string
}

variable "partition" {
  type    = string
  default = "aws"
}

variable "oidc_provider_arn" {
  type = string
}

variable "oidc_provider_url" {
  description = "OIDC provider URL without https:// prefix"
  type        = string
}

variable "iceberg_bucket_arn" {
  type = string
}

variable "checkpoints_bucket_arn" {
  type = string
}

variable "artifacts_bucket_arn" {
  type = string
}

variable "logs_bucket_arn" {
  type = string
}

variable "glue_database_arns" {
  description = "ARNs for Glue catalog + databases + tables (Iceberg catalog)"
  type        = list(string)
}

variable "spark_namespace" {
  type    = string
  default = "spark"
}

variable "spark_service_account" {
  type    = string
  default = "spark"
}

variable "spark_operator_namespace" {
  type    = string
  default = "spark-operator"
}

variable "spark_operator_service_account" {
  type    = string
  default = "spark-operator"
}

variable "airflow_namespace" {
  type    = string
  default = "airflow"
}

variable "airflow_service_account" {
  type    = string
  default = "airflow"
}

variable "autoscaler_namespace" {
  type    = string
  default = "kube-system"
}

variable "autoscaler_service_account" {
  type    = string
  default = "cluster-autoscaler"
}
