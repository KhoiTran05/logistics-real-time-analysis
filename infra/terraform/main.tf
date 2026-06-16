# ─────────────────────────────────────────────────────────────────────────────
# Providers
# NOTE: The kubernetes and helm providers need the EKS cluster to exist first.
# On a fresh deploy, run in two phases — see docs/deployment.md.
# ─────────────────────────────────────────────────────────────────────────────

provider "aws" {
  region = var.aws_region
  default_tags { tags = local.common_tags }
}

provider "kubernetes" {
  host                   = try(module.eks.cluster_endpoint, "")
  cluster_ca_certificate = try(base64decode(module.eks.cluster_ca_certificate), "")
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", local.eks_cluster_name, "--region", var.aws_region]
  }
}

provider "helm" {
  kubernetes {
    host                   = try(module.eks.cluster_endpoint, "")
    cluster_ca_certificate = try(base64decode(module.eks.cluster_ca_certificate), "")
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", local.eks_cluster_name, "--region", var.aws_region]
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# ── Phase 1: Core Infrastructure ─────────────────────────────────────────────

module "vpc" {
  source = "./modules/vpc"

  name_prefix          = local.name_prefix
  vpc_cidr             = var.vpc_cidr
  availability_zones   = ["ap-southeast-1a", "ap-southeast-1b"]
  public_subnet_cidrs  = ["10.0.0.0/24", "10.0.1.0/24"]
  private_subnet_cidrs = ["10.0.10.0/24", "10.0.11.0/24"]
  eks_cluster_name     = local.eks_cluster_name
}

module "security_groups" {
  source = "./modules/security_groups"

  name_prefix = local.name_prefix
  vpc_id      = module.vpc.vpc_id
  vpc_cidr    = var.vpc_cidr
}

module "iam" {
  source = "./modules/iam"

  name_prefix      = local.name_prefix
  eks_cluster_name = local.eks_cluster_name
  account_id       = data.aws_caller_identity.current.account_id
  partition        = data.aws_partition.current.partition
}

module "s3" {
  source = "./modules/s3"

  name_prefix = local.name_prefix
  glue_prefix = local.glue_prefix
  aws_region  = var.aws_region
  account_id  = data.aws_caller_identity.current.account_id
}

module "ecr" {
  source = "./modules/ecr"

  name_prefix  = local.name_prefix
  repositories = ["spark", "kafka-producer"]
}

module "eks" {
  source = "./modules/eks"

  cluster_name       = local.eks_cluster_name
  kubernetes_version = var.kubernetes_version
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  cluster_sg_id      = module.security_groups.eks_cluster_sg_id
  node_sg_id         = module.security_groups.eks_node_sg_id
  cluster_role_arn   = module.iam.eks_cluster_role_arn
  node_role_arn      = module.iam.eks_node_role_arn
  node_groups        = var.eks_node_groups
}

module "irsa" {
  source = "./modules/irsa"

  name_prefix            = local.name_prefix
  account_id             = data.aws_caller_identity.current.account_id
  partition              = data.aws_partition.current.partition
  oidc_provider_arn      = module.eks.oidc_provider_arn
  oidc_provider_url      = module.eks.oidc_provider_url
  iceberg_bucket_arn     = module.s3.iceberg_bucket_arn
  checkpoints_bucket_arn = module.s3.checkpoints_bucket_arn
  artifacts_bucket_arn   = module.s3.artifacts_bucket_arn
  logs_bucket_arn        = module.s3.logs_bucket_arn
  glue_database_arns     = module.s3.glue_database_arns
}

# ── Phase 2: Platform (requires EKS cluster) ──────────────────────────────────

module "helm_releases" {
  source = "./modules/helm_releases"

  name_prefix                      = local.name_prefix
  aws_region                       = var.aws_region
  iceberg_bucket_name              = module.s3.iceberg_bucket_name
  checkpoints_bucket_name          = module.s3.checkpoints_bucket_name
  artifacts_bucket_name            = module.s3.artifacts_bucket_name
  spark_irsa_role_arn              = module.irsa.spark_role_arn
  airflow_irsa_role_arn            = module.irsa.airflow_role_arn
  cluster_autoscaler_irsa_role_arn = module.irsa.cluster_autoscaler_role_arn
  cluster_name                     = module.eks.cluster_name
  dags_git_repo                    = var.dags_git_repo
  dags_git_branch                  = var.dags_git_branch
  dags_git_subpath                 = var.dags_git_subpath

  depends_on = [module.eks, module.irsa, module.s3]
}

module "data_generator" {
  source = "./modules/data_generator"

  name_prefix       = local.name_prefix
  aws_region        = var.aws_region
  private_subnet_id = module.vpc.private_subnet_id
  security_group_id = module.security_groups.generator_sg_id
  instance_type     = var.generator_instance_type
  key_pair_name     = var.generator_key_pair_name
  # Kafka NodePort bootstrap — generator connects via EKS node IPs
  eks_cluster_name = local.eks_cluster_name
  kafka_nodeport   = 32092
  # Generator pulls simulation/ scripts from the artifacts bucket at boot
  artifacts_bucket = module.s3.artifacts_bucket_name

  depends_on = [module.helm_releases]
}

# Kafka NodePort access: data generator EC2 → EKS managed nodes.
# Managed node groups attach only the EKS-created cluster SG (not our custom node SG),
# so the NodePort ingress must live on that SG. Range covers Strimzi's bootstrap port
# (32092) and the dynamically-assigned per-broker NodePorts.
resource "aws_security_group_rule" "generator_to_nodes_kafka_nodeport" {
  type                     = "ingress"
  from_port                = 30000
  to_port                  = 32767
  protocol                 = "tcp"
  security_group_id        = module.eks.cluster_security_group_id
  source_security_group_id = module.security_groups.generator_sg_id
  description              = "Kafka NodePort from data generator"
}
