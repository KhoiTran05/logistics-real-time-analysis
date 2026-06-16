variable "name_prefix" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "private_subnet_id" {
  type = string
}

variable "security_group_id" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3.micro"
}

variable "key_pair_name" {
  description = "EC2 key pair for SSH. Leave empty to use SSM Session Manager only."
  type        = string
  default     = ""
}

variable "eks_cluster_name" {
  description = "EKS cluster name — used to discover node IPs for Kafka NodePort"
  type        = string
}

variable "artifacts_bucket" {
  description = "S3 artifacts bucket — generator pulls simulation/ scripts from here at boot"
  type        = string
}

variable "generator_rate" {
  description = "Shipments created per second by the generator (~15 events each). 6 ≈ ~90 msg/s, matched to a 1-broker Kafka + 1-2 small Spark executors."
  type        = number
  default     = 6
}

variable "kafka_nodeport" {
  description = "NodePort exposed by Strimzi for external access"
  type        = number
  default     = 32092
}
