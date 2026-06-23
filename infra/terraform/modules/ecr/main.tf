resource "aws_ecr_repository" "repos" {
  for_each             = toset(var.repositories)
  name                 = "${var.name_prefix}/${each.value}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }

  tags = { Name = "${var.name_prefix}/${each.value}" }
}

resource "aws_ecr_lifecycle_policy" "repos" {
  for_each   = toset(var.repositories)
  repository = aws_ecr_repository.repos[each.key].name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}
