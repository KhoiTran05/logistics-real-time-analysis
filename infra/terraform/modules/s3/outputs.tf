output "iceberg_bucket_name" {
  value = aws_s3_bucket.buckets["iceberg"].id
}

output "iceberg_bucket_arn" {
  value = aws_s3_bucket.buckets["iceberg"].arn
}

output "checkpoints_bucket_name" {
  value = aws_s3_bucket.buckets["checkpoints"].id
}

output "checkpoints_bucket_arn" {
  value = aws_s3_bucket.buckets["checkpoints"].arn
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.buckets["artifacts"].id
}

output "artifacts_bucket_arn" {
  value = aws_s3_bucket.buckets["artifacts"].arn
}

output "logs_bucket_name" {
  value = aws_s3_bucket.buckets["logs"].id
}

output "logs_bucket_arn" {
  value = aws_s3_bucket.buckets["logs"].arn
}

output "glue_db_bronze" {
  value = aws_glue_catalog_database.bronze.name
}

output "glue_db_silver" {
  value = aws_glue_catalog_database.silver.name
}

output "glue_db_gold" {
  value = aws_glue_catalog_database.gold.name
}

output "glue_database_arns" {
  description = "ARNs for all 3 Glue databases (used in IRSA policy)"
  value = [
    "arn:aws:glue:*:*:catalog",
    "arn:aws:glue:*:*:database/${aws_glue_catalog_database.bronze.name}",
    "arn:aws:glue:*:*:database/${aws_glue_catalog_database.silver.name}",
    "arn:aws:glue:*:*:database/${aws_glue_catalog_database.gold.name}",
    "arn:aws:glue:*:*:table/${aws_glue_catalog_database.bronze.name}/*",
    "arn:aws:glue:*:*:table/${aws_glue_catalog_database.silver.name}/*",
    "arn:aws:glue:*:*:table/${aws_glue_catalog_database.gold.name}/*",
  ]
}
