#!/bin/bash
set -euo pipefail

# Builds the custom spark-operator image (upstream operator + hadoop-aws) and pushes
# it to ECR. Must run AFTER the ECR repo exists (terraform Phase 1) and BEFORE the
# spark-operator Helm release deploys (terraform Phase 2), otherwise the operator
# pod ImagePullBackOffs on the not-yet-pushed image.

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-southeast-1"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
OPERATOR_REPO="vdt-logistics-dev/spark-operator"
OPERATOR_TAG="v1beta2-1.4.6-3.5.0"
OPERATOR_IMAGE="${ECR_REGISTRY}/${OPERATOR_REPO}:${OPERATOR_TAG}"

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable." >&2
  echo "Start Docker Desktop, then rerun this script." >&2
  exit 1
fi

if ! aws ecr describe-repositories \
  --repository-names "${OPERATOR_REPO}" \
  --region "${REGION}" \
  >/dev/null 2>&1; then
  echo "ECR repository '${OPERATOR_REPO}' does not exist." >&2
  echo "Create it with Terraform first: cd infra/terraform && terraform apply -target=module.ecr" >&2
  exit 1
fi

aws ecr get-login-password --region "${REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "----------------------------------------"
echo "Building Spark Operator image..."
docker build -f infra/ecr/Dockerfile.spark-operator -t "${OPERATOR_IMAGE}" .
echo "Pushing Spark Operator image..."
docker push "${OPERATOR_IMAGE}"
