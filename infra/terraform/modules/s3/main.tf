locals {
  buckets = {
    iceberg     = "${var.name_prefix}-iceberg-${var.account_id}"
    checkpoints = "${var.name_prefix}-checkpoints-${var.account_id}"
    artifacts   = "${var.name_prefix}-artifacts-${var.account_id}"
    logs        = "${var.name_prefix}-logs-${var.account_id}"
  }
}

# ── S3 Buckets ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "buckets" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true
  tags          = { Purpose = each.key }
}

resource "aws_s3_bucket_versioning" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "checkpoints" {
  bucket = aws_s3_bucket.buckets["checkpoints"].id

  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    filter {}
    expiration { days = 14 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.buckets["logs"].id

  rule {
    id     = "tiered-expiry"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration { days = 60 }
  }
}

# Iceberg warehouse prefix structure
resource "aws_s3_object" "iceberg_prefixes" {
  for_each = toset([
    "warehouse/bronze/",
    "warehouse/silver/",
    "warehouse/gold/",
  ])
  bucket  = aws_s3_bucket.buckets["iceberg"].id
  key     = each.value
  content = ""
}

# ── AWS Glue Catalog Databases (Iceberg catalog via Glue) ────────────────────

resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.glue_prefix}_bronze"
  description = "Iceberg Bronze — raw streaming events"

  location_uri = "s3://${aws_s3_bucket.buckets["iceberg"].id}/warehouse/bronze/"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${var.glue_prefix}_silver"
  description = "Iceberg Silver — cleaned, enriched events"

  location_uri = "s3://${aws_s3_bucket.buckets["iceberg"].id}/warehouse/silver/"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${var.glue_prefix}_gold"
  description = "Iceberg Gold — KPI aggregates, anomaly alerts"

  location_uri = "s3://${aws_s3_bucket.buckets["iceberg"].id}/warehouse/gold/"
}
