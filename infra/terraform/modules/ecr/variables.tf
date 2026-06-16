variable "name_prefix" {
  type = string
}

variable "repositories" {
  description = "List of ECR repository short names"
  type        = list(string)
}
