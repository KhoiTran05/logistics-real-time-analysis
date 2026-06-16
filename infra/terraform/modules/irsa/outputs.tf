output "spark_role_arn" {
  value = aws_iam_role.spark.arn
}

output "airflow_role_arn" {
  value = aws_iam_role.airflow.arn
}

output "cluster_autoscaler_role_arn" {
  value = aws_iam_role.cluster_autoscaler.arn
}
