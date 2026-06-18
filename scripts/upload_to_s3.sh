#!/bin/bash
set -euo pipefail

ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)

aws s3 sync src/ "s3://${ARTIFACTS}/src/" --exclude "__pycache__/*"

