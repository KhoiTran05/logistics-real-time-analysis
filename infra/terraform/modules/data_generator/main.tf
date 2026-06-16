data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    # Standard AL2023 only — the "minimal" variant ships without the SSM Agent,
    # which breaks Session Manager access (TargetNotConnected).
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_iam_role" "generator" {
  name = "${var.name_prefix}-generator-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# SSM Session Manager — access without SSH key
resource "aws_iam_role_policy_attachment" "generator_ssm" {
  role       = aws_iam_role.generator.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Discover EKS node IPs for Kafka NodePort bootstrap
resource "aws_iam_role_policy" "generator_ec2_read" {
  role = aws_iam_role.generator.name
  name = "ec2-describe-for-kafka-discovery"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ec2:DescribeInstances"]
      Resource = "*"
    }]
  })
}

# Pull simulation/ scripts (catalog + event_generator) from the artifacts bucket
resource "aws_iam_role_policy" "generator_s3_read" {
  role = aws_iam_role.generator.name
  name = "s3-read-artifacts"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::${var.artifacts_bucket}",
        "arn:aws:s3:::${var.artifacts_bucket}/*",
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "generator" {
  name = "${var.name_prefix}-generator-profile"
  role = aws_iam_role.generator.name
}

resource "aws_instance" "generator" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = var.private_subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = aws_iam_instance_profile.generator.name
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null

  user_data = base64encode(templatefile("${path.module}/user_data.sh", {
    aws_region       = var.aws_region
    eks_cluster_name = var.eks_cluster_name
    kafka_nodeport   = var.kafka_nodeport
    artifacts_bucket = var.artifacts_bucket
    generator_rate   = var.generator_rate
  }))

  tags = { Name = "${var.name_prefix}-data-generator" }

  lifecycle { ignore_changes = [ami] }
}
