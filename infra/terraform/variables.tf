variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "vdt-logistics"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-1"
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

# Single AZ for demo — saves ~$32/month on second NAT Gateway
variable "availability_zone" {
  description = "Single AZ for demo deployment"
  type        = string
  default     = "ap-southeast-1a"
}

variable "public_subnet_cidr" {
  type    = string
  default = "10.0.0.0/24"
}

variable "private_subnet_cidr" {
  type    = string
  default = "10.0.10.0/24"
}

# ── EKS ───────────────────────────────────────────────────────────────────────

variable "kubernetes_version" {
  type    = string
  default = "1.30"
}

# Free-tier instance cap: m7i-flex.large (2 vCPU / 8 GiB) or c7i-flex.large
# (2 vCPU / 4 GiB) or smaller.
#
# Cluster Autoscaler IS installed (modules/helm_releases). So min/max are the
# scaling RANGE and desired_size is only the initial count — CA grows each pool up
# to max_size whenever pods are Pending and shrinks back to min_size when idle.
# Because the cluster runs only ~3-4h/day, max_size is set generously for comfort
# (never "just enough"): CA adds nodes on demand and removes them when unused.
#
# A single 2-vCPU node has ~1.6 vCPU allocatable after system daemonsets. Stateful
# pods (Kafka/ClickHouse) need 8 GiB, so general is m7i-flex.large only.
# Managed node groups require a SINGLE instance type when capacity_type is
# ON_DEMAND (multiple types are only allowed for SPOT).
variable "eks_node_groups" {
  type = map(object({
    instance_types = list(string)
    min_size       = number
    max_size       = number
    desired_size   = number
    disk_size_gb   = number
    capacity_type  = string
    labels         = map(string)
    taints = list(object({
      key    = string
      value  = string
      effect = string
    }))
  }))
  default = {
    # General pool: Kafka (Strimzi), Airflow, Grafana, ClickHouse, operators,
    # system pods (~2.9 vCPU / ~5.8 GiB requests). Steady state = 2 nodes
    # (~25% CPU headroom); CA can add up to 4 for bursts / batch.
    general = {
      instance_types = ["m7i-flex.large"]
      min_size       = 2
      max_size       = 4
      desired_size   = 2
      disk_size_gb   = 50
      capacity_type  = "ON_DEMAND"
      labels         = { "node-type" = "general" }
      taints         = []
    }
    # Spark pool: driver + executors, 1 pod per 2-vCPU node. Starts at 1 (keeps the
    # streaming driver's node warm); CA scales to max_size when executors are
    # submitted — so executors never sit Pending. Bump max for heavier batch runs.
    spark = {
      instance_types = ["m7i-flex.large"]
      min_size       = 1
      max_size       = 5
      desired_size   = 2
      disk_size_gb   = 60
      capacity_type  = "ON_DEMAND"
      labels         = { "node-type" = "spark" }
      taints = [{
        key    = "dedicated"
        value  = "spark"
        effect = "NO_SCHEDULE"
      }]
    }
  }
}

# ── Helm / Platform ───────────────────────────────────────────────────────────

variable "dags_git_repo" {
  description = "Git repo URL for Airflow DAGs (gitSync)"
  type        = string
  default     = "https://github.com/KhoiTran05/logistics-real-time-analysis"
}

variable "dags_git_branch" {
  description = "Branch for Airflow DAG gitSync"
  type        = string
  default     = "main"
}

variable "dags_git_subpath" {
  description = "Subdirectory in the git repo containing DAGs"
  type        = string
  default     = "airflow/dags"
}

# ── Data Generator ────────────────────────────────────────────────────────────

variable "generator_instance_type" {
  description = "EC2 instance type for the Python data generator"
  type        = string
  default     = "t3.micro"
}

variable "generator_key_pair_name" {
  description = "EC2 key pair name for SSH access to the generator (leave empty to skip)"
  type        = string
  default     = ""
}
