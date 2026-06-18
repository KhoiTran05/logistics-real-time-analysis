#!/bin/bash
set -euo pipefail

ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)

aws s3 sync src/ "s3://${ARTIFACTS}/src/" --exclude "__pycache__/*"

# Package src/ as a zip so Spark can ship it to executors via spec.deps.pyFiles
rm -f src.zip
python -c "
import os
import zipfile

with zipfile.ZipFile('src.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, dirs, files in os.walk('src'):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for name in files:
            if name.endswith('.pyc'):
                continue
            path = os.path.join(root, name)
            zf.write(path, path)
"
aws s3 cp src.zip "s3://${ARTIFACTS}/src.zip"
rm -f src.zip

