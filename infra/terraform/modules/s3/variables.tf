variable "name_prefix" {
  type = string
}

variable "glue_prefix" {
  description = "Hyphen-free version of name_prefix for Glue database names"
  type        = string
}

variable "aws_region" {
  type = string
}

variable "account_id" {
  type = string
}
