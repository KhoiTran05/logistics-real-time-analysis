locals {
  name_prefix      = "${var.project_name}-${var.environment}"
  eks_cluster_name = "${local.name_prefix}-eks"

  # Glue DB names cannot have hyphens
  glue_prefix = replace(local.name_prefix, "-", "_")

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Team        = "data-engineering"
  }
}
