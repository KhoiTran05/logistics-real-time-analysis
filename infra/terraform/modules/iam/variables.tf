variable "name_prefix" {
  type = string
}

variable "eks_cluster_name" {
  type = string
}

variable "account_id" {
  type = string
}

variable "partition" {
  type    = string
  default = "aws"
}
