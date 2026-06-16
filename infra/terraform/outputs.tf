output "kubeconfig_command" {
  description = "Command to update local kubeconfig"
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}

output "eks_cluster_name" {
  value = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  value     = module.eks.cluster_endpoint
  sensitive = true
}

output "s3_iceberg_bucket" {
  value = module.s3.iceberg_bucket_name
}

output "s3_checkpoints_bucket" {
  value = module.s3.checkpoints_bucket_name
}

output "s3_artifacts_bucket" {
  value = module.s3.artifacts_bucket_name
}

output "glue_databases" {
  description = "Glue Catalog databases for Iceberg (bronze / silver / gold)"
  value = {
    bronze = module.s3.glue_db_bronze
    silver = module.s3.glue_db_silver
    gold   = module.s3.glue_db_gold
  }
}

output "spark_irsa_role_arn" {
  value = module.irsa.spark_role_arn
}

output "airflow_irsa_role_arn" {
  value = module.irsa.airflow_role_arn
}

output "clickhouse_password_secret" {
  description = "Secrets Manager secret name for ClickHouse credentials"
  value       = module.helm_releases.clickhouse_secret_name
}

output "grafana_password_secret" {
  description = "Secrets Manager secret name for Grafana admin credentials"
  value       = module.helm_releases.grafana_secret_name
}

output "generator_instance_id" {
  description = "EC2 instance ID for the data generator"
  value       = module.data_generator.instance_id
}

output "ecr_urls" {
  value = module.ecr.repository_urls
}
