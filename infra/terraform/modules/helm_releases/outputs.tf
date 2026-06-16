output "kafka_bootstrap_internal" {
  description = "Kafka bootstrap for pods inside EKS"
  value       = "${var.kafka_cluster_name}-kafka-bootstrap.kafka.svc.cluster.local:9092"
}

output "kafka_bootstrap_nodeport" {
  description = "Kafka bootstrap for EC2 generator (NodePort — use any EKS node IP:32092)"
  value       = "ANY_EKS_NODE_IP:32092"
}

output "clickhouse_host_internal" {
  description = "ClickHouse host for Spark JDBC writes"
  value       = "clickhouse.clickhouse.svc.cluster.local"
}

output "clickhouse_secret_name" {
  value = aws_secretsmanager_secret.clickhouse.name
}

output "grafana_secret_name" {
  value = aws_secretsmanager_secret.grafana.name
}
