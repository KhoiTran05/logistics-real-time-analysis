#!/bin/bash
set -euo pipefail

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.ap-southeast-1.amazonaws.com"
SPARK_IMAGE="${ECR_REGISTRY}/vdt-logistics-dev/spark:3.5.1"
SPARK_REPO="vdt-logistics-dev/spark"
REGION="ap-southeast-1"

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable." >&2
  echo "Start Docker Desktop, then rerun this script." >&2
  echo "If the image already exists in ECR, rerun with SKIP_IMAGE_BUILD=true." >&2
  exit 1
fi

echo "Ensuring ECR repositories exist..."
for REPO in "${SPARK_REPO}"; do
  if ! aws ecr describe-repositories \
    --repository-names "${REPO}" \
    --region "${REGION}" \
    >/dev/null 2>&1; then
    echo "ECR repository '${REPO}' does not exist." >&2
    echo "Create it with Terraform first: cd infra/terraform && terraform apply" >&2
    exit 1
  fi
  echo " - Repository '${REPO}' is ready."
done

aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "----------------------------------------"
echo "Building Spark image..."
docker build -f infra/ecr/Dockerfile.spark -t "${SPARK_IMAGE}" .
echo "Pushing Spark image..."
docker push "${SPARK_IMAGE}"