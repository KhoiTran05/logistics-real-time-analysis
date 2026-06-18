# ── Spark IRSA Role ───────────────────────────────────────────────────────────

resource "aws_iam_role" "spark" {
  name        = "${var.name_prefix}-spark-irsa"
  description = "Assumed by Spark pods in EKS via IRSA"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          # Both the Spark driver/executor SA and the spark-operator SA assume this
          # role: the operator's in-process spark-submit reads the s3a:// app file.
          "${var.oidc_provider_url}:sub" = [
            "system:serviceaccount:${var.spark_namespace}:${var.spark_service_account}",
            "system:serviceaccount:${var.spark_operator_namespace}:${var.spark_operator_service_account}",
          ]
          "${var.oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "spark_s3_iceberg" {
  role = aws_iam_role.spark.name
  name = "s3-iceberg-rw"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "IcebergRW"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
          "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
        ]
        Resource = [
          var.iceberg_bucket_arn, "${var.iceberg_bucket_arn}/*",
          var.checkpoints_bucket_arn, "${var.checkpoints_bucket_arn}/*",
          var.logs_bucket_arn, "${var.logs_bucket_arn}/*",
        ]
      },
      {
        Sid      = "ArtifactsRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [var.artifacts_bucket_arn, "${var.artifacts_bucket_arn}/*"]
      }
    ]
  })
}

# Iceberg needs Glue Catalog to manage table metadata
resource "aws_iam_role_policy" "spark_glue" {
  role = aws_iam_role.spark.name
  name = "glue-iceberg-catalog"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "GlueCatalog"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
        "glue:BatchCreatePartition", "glue:BatchDeletePartition",
        "glue:BatchGetPartition"
      ]
      Resource = var.glue_database_arns
    }]
  })
}

resource "aws_iam_role_policy" "spark_cloudwatch" {
  role = aws_iam_role.spark.name
  name = "cloudwatch-metrics"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["cloudwatch:PutMetricData"]
      Resource = "*"
    }]
  })
}

# ── Airflow IRSA Role ─────────────────────────────────────────────────────────

resource "aws_iam_role" "airflow" {
  name        = "${var.name_prefix}-airflow-irsa"
  description = "Assumed by Airflow pods in EKS via IRSA"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.oidc_provider_url}:sub" = "system:serviceaccount:${var.airflow_namespace}:${var.airflow_service_account}"
          "${var.oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "airflow_s3" {
  role = aws_iam_role.airflow.name
  name = "s3-read"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [var.artifacts_bucket_arn, "${var.artifacts_bucket_arn}/*",
      var.iceberg_bucket_arn, "${var.iceberg_bucket_arn}/*"]
    }]
  })
}

resource "aws_iam_role_policy" "airflow_eks" {
  role = aws_iam_role.airflow.name
  name = "eks-describe"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["eks:DescribeCluster"]
      Resource = "*"
    }]
  })
}

# ── Cluster Autoscaler IRSA Role ──────────────────────────────────────────────

resource "aws_iam_role" "cluster_autoscaler" {
  name        = "${var.name_prefix}-cluster-autoscaler-irsa"
  description = "Assumed by the cluster-autoscaler pod via IRSA"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = var.oidc_provider_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.oidc_provider_url}:sub" = "system:serviceaccount:${var.autoscaler_namespace}:${var.autoscaler_service_account}"
          "${var.oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  role = aws_iam_role.cluster_autoscaler.name
  name = "autoscaling-management"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "autoscaling:DescribeAutoScalingGroups",
        "autoscaling:DescribeAutoScalingInstances",
        "autoscaling:DescribeLaunchConfigurations",
        "autoscaling:DescribeScalingActivities",
        "autoscaling:DescribeTags",
        "autoscaling:SetDesiredCapacity",
        "autoscaling:TerminateInstanceInAutoScalingGroup",
        "ec2:DescribeInstanceTypes",
        "ec2:DescribeLaunchTemplateVersions",
        "ec2:DescribeImages",
        "ec2:GetInstanceTypesFromInstanceRequirements",
        "eks:DescribeNodegroup",
      ]
      Resource = "*"
    }]
  })
}
