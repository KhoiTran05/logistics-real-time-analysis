# Deployment Guide — VDT Logistics Realtime Pipeline (AWS, Student Demo)

## Architecture Overview

```
EC2 t3.micro (Python generator)
         │  NodePort 32092
         ▼
Kafka (Strimzi/KRaft, 1 broker, on EKS)
         │  ClusterIP 9092
         ▼
Spark Structured Streaming (EKS Spot nodes)
  ├── Kafka → Bronze Iceberg → Silver Iceberg
  ├── Silver → KPI aggregation → Gold Iceberg
  ├── Gold → ClickHouse sink (JDBC)
  └── Silver → Anomaly detection → ClickHouse
         │
         ▼
 S3 + Glue Catalog (Iceberg warehouse)
  ├── Bronze: raw events
  ├── Silver: cleaned + enriched
  └── Gold: KPI aggregates

ClickHouse (on EKS, serving layer)
         │
Grafana (on EKS, ClickHouse datasource)

Airflow (on EKS, triggers batch Spark jobs)
```

## Demo Compromises 

| Component | Production | Demo |
|---|---|---|
| Kafka | MSK multi-broker HA | Strimzi 1 broker on EKS — saves ~$100/month |
| EKS nodes | On-Demand 5 groups | general 2–4× m7i-flex.large + spark 1–5× m7i-flex.large, On-Demand, free-tier cap; **Cluster Autoscaler** auto-scales on Pending pods (runs ~3-4h/day so max is set generously) |
| Availability | Multi-AZ | Single AZ — saves ~$32/month (NAT GW) |
| Kafka auth | TLS + SASL | Plain (no auth, private VPC) |
| Airflow | CeleryExecutor + Redis | LocalExecutor (tasks sequential) |
| Kafka partitions | 64/32/16 | 8/4/2 (enough for 200 msg/s demo) |

---

## Cost Estimate

| Service | Configuration | USD/month |
|---|---|---|
| EKS Control Plane | 1 cluster | $73 (24/7) · ~$12 (3-4h/day) |
| EC2 — general (Kafka/Airflow/Grafana/CH) | 2–4× m7i-flex.large On-Demand | ~$155/month (24/7) · **~$20 (3-4h/day)** |
| EC2 — spark (autoscaled) | 1–5× m7i-flex.large On-Demand | ~$8/month (3-4h/day, scale-to-min when idle) |
| EC2 t3.micro (generator) | 1× On-Demand | $8 |
| NAT Gateway | 1× | $32 |
| S3 | ~200 GB | $5 |
| Glue Catalog | 3 databases | <$1 |
| **Total (24/7)** | | **~$260/month** |
| **Total (destroy/recreate ~3-4h/day)** | | **~$65–75/month** |

> **~3-4h/day mode (recommended):** `terraform destroy` after each session → nearly all costs are usage-based (EKS control plane, EC2 general+spark, NAT GW are billed only for runtime). Cluster Autoscaler automatically shrinks the spark pool to `min_size` when there's no job, so you don't pay for idle spark nodes. Only S3 (~$5) is a fixed cost.

---

## Prerequisites

```bash
# Check required tools
terraform version     
aws --version         # >= 2.x
kubectl version --client
helm version          # >= 3.12
```

AWS Credentials:
```bash
aws configure
# Or export AWS_PROFILE=my-profile
aws sts get-caller-identity
```

---

## Step 1 — Bootstrap Terraform State Backend

```bash
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-southeast-1"

# S3 bucket for state
aws s3api create-bucket \
  --bucket "vdt-terraform-state-${AWS_ACCOUNT_ID}" \
  --region "${REGION}" \
  --create-bucket-configuration LocationConstraint="${REGION}"

aws s3api put-bucket-versioning \
  --bucket "vdt-terraform-state-${AWS_ACCOUNT_ID}" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "vdt-terraform-state-${AWS_ACCOUNT_ID}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket "vdt-terraform-state-${AWS_ACCOUNT_ID}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# DynamoDB lock table
aws dynamodb create-table \
  --table-name "vdt-terraform-locks" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "${REGION}"

# Create backend.tfvars
cat > infra/terraform/backend.tfvars << EOF
bucket         = "vdt-terraform-state-${AWS_ACCOUNT_ID}"
key            = "vdt-mini-project/terraform.tfstate"
region         = "ap-southeast-1"
encrypt        = true
use_lockfile   = true
EOF
```

---

## Step 2 — Apply Phase 1: Core Infrastructure

Since Terraform cannot configure the Kubernetes/Helm provider before the EKS cluster exists, apply needs to happen in 2 phases.

```bash
cd infra/terraform

# Initialize Terraform
terraform init -backend-config=backend.tfvars

# View plan
terraform plan \
  -target=module.vpc \
  -target=module.security_groups \
  -target=module.iam \
  -target=module.eks \
  -target=module.s3 \
  -target=module.ecr \
  -target=module.irsa

# Phase 1: Create VPC, EKS, IAM, S3 (~15-20 min)
terraform apply \
  -target=module.vpc \
  -target=module.security_groups \
  -target=module.iam \
  -target=module.eks \
  -target=module.s3 \
  -target=module.ecr \
  -target=module.irsa

# After Phase 1 finishes, update kubeconfig
$(terraform output -raw kubeconfig_command)
kubectl get nodes
```

---

## Step 2b — Build & Push Spark Operator Image

> **Required before Phase 2.** Spark Operator runs `spark-submit` directly inside its own pod,
> and in `mode: cluster` it resolves `s3a://` mainApplicationFile/pyFiles locally — it needs
> `hadoop-aws` on the operator's own classpath (the upstream image doesn't have it). We use a
> custom operator image (`infra/ecr/Dockerfile.spark-operator`). The image must be on ECR
> **before** the spark-operator Helm release deploys in Phase 2, otherwise the operator pod will
> hit `ImagePullBackOff`. The ECR repo `vdt-logistics-dev/spark-operator` was already created in Phase 1.

```bash
cd ../..
./scripts/ecr-push-spark-operator.sh
```

---

## Step 3 — Apply Phase 2: Platform 

```bash
cd infra/terraform

# Phase 2: Deploy the full platform (~20-30 min)
# Automatic order: Strimzi → Kafka cluster → Topics → Airflow → ClickHouse → Schema → Grafana
terraform apply -target=module.helm_releases.helm_release.strimzi

terraform apply

# Check all pods
kubectl get pods -A

# Check Kafka is running
kubectl get kafka -n kafka
kubectl get kafkatopic -n kafka

# Check ClickHouse
kubectl get pods -n clickhouse

# Check Grafana
kubectl get pods -n monitoring
```

---

## Step 4 — Build and Push Spark Image

```bash
cd ../..

# Build and push Spark image
./scripts/ecr-push-spark-runtime.sh

# Upload Spark scripts to S3 (sync src/ + package src.zip for spec.deps.pyFiles)
./scripts/upload_to_s3.sh
```

---

## Step 4b — Seed Dim Tables + Run Spark Job (dim_tables_create)

`simulation/catalog.py` is the **single source of IDs**: the same data is written to the Dim
tables by the seeder and used by the event generator to produce events → every event references
a real, valid ID from the Dim tables (see [streaming_events_schema.md](streaming_events_schema.md)).

```bash
ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)

source venv/Scripts/activate
pip install -r simulation/requirements.txt

# Seed Dim tables (run ONCE) -> s3://${ARTIFACTS}/dim-seed/<table>/<table>.csv
python simulation/dim_seeder.py --s3-bucket "${ARTIFACTS}"

# Render Spark Job Application
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ICEBERG_BUCKET=$(cd infra/terraform && terraform output -raw s3_iceberg_bucket)
CHECKPOINTS_BUCKET=$(cd infra/terraform && terraform output -raw s3_checkpoints_bucket)
SPARK_IMAGE="${AWS_ACCOUNT_ID}.dkr.ecr.ap-southeast-1.amazonaws.com/vdt-logistics-dev/spark:3.5.1"

python -m src.utils.render_application \
  --app_template_path configs/spark/app-template.yaml \
  --app_config_path   configs/spark/app-config.yaml \
  --job_name          dim_tables_create \
  --target_path       configs/_rendered/dim_tables_create.yaml \
  --spark_image       "$SPARK_IMAGE" \
  --iceberg_bucket    "$ICEBERG_BUCKET" \
  --checkpoints_bucket "$CHECKPOINTS_BUCKET"

# Run Spark Job
kubectl apply -f configs/_rendered/dim_tables_create.yaml

# Checking Spark Application creating
kubectl get sparkapplication -n spark

# Checking Spark Job running (Driver and Executor running)
kubectl get pods -n spark
```
---

## Step 4c: Upload Generator Scripts

```bash
# Upload scripts for the EC2 generator (EC2 auto-syncs s3://.../simulation/ on every start)
aws s3 sync simulation/ "s3://${ARTIFACTS}/simulation/" --exclude "__pycache__/*"

# Check Kafka topics and messages
kubectl exec -it logistics-kafka-combined-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

kubectl exec -it logistics-kafka-combined-0 -n kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic logistics.tracking.events

kubectl exec -it logistics-kafka-combined-0 -n kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic logistics.tracking.events \
  --from-beginning \
  --max-messages 5
```

> The EC2 generator boots during Phase 2 (Step 3) **before** the scripts exist on S3 — that's fine:
> the service fails and systemd retries every 30s, and once the `aws s3 sync` above completes it starts automatically.

---

## Step 5 — Submit Spark Streaming Job

```bash
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ICEBERG_BUCKET=$(cd infra/terraform && terraform output -raw s3_iceberg_bucket)
CHECKPOINTS_BUCKET=$(cd infra/terraform && terraform output -raw s3_checkpoints_bucket)
SPARK_IMAGE="${AWS_ACCOUNT_ID}.dkr.ecr.ap-southeast-1.amazonaws.com/vdt-logistics-dev/spark:3.5.1"

python -m src.utils.render_application \
  --app_template_path configs/spark/app-template.yaml \
  --app_config_path   configs/spark/app-config.yaml \
  --job_name          streaming \
  --target_path       configs/_rendered/streaming.yaml \
  --spark_image       "$SPARK_IMAGE" \
  --iceberg_bucket    "$ICEBERG_BUCKET" \
  --checkpoints_bucket "$CHECKPOINTS_BUCKET"

# Run Spark Job
kubectl apply -f configs/_rendered/streaming.yaml
# Monitor
kubectl get sparkapplication -n spark
kubectl logs -l spark-role=driver -n spark -f
```

---

## Step 6 — Access Dashboards

```bash
# Port-forward Grafana
kubectl port-forward svc/grafana 3000:80 -n monitoring &
# Access: http://localhost:3000
# Credentials:
aws secretsmanager get-secret-value \
  --secret-id $(cd infra/terraform && terraform output -raw grafana_password_secret) \
  --query SecretString --output text | python3 -m json.tool

# Port-forward Airflow
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow &
# Access: http://localhost:8080 (admin/admin by default)

# View ClickHouse directly
kubectl exec -it -n clickhouse \
  $(kubectl get pod -n clickhouse -l app.kubernetes.io/name=clickhouse -o jsonpath='{.items[0].metadata.name}') \
  -- clickhouse-client --user admin --database logistics
```

---

## Step 7 — Check the Generator EC2 Instance

```bash
GENERATOR_ID=$(cd infra/terraform && terraform output -raw generator_instance_id)

# Connect via SSM (no SSH key needed)
aws ssm start-session --target "${GENERATOR_ID}" --region ap-southeast-1

# Inside EC2:
cat /opt/kafka-producer.env
ls /opt/simulation                 # catalog.py, event_generator.py, ...
systemctl status kafka-producer
journalctl -u kafka-producer -f    # "[Ns] created=… sent=… (~N msg/s)"
```

> If the generator isn't sending events: check whether the scripts are on S3
> (`aws s3 ls s3://${ARTIFACTS}/simulation/`), then `sudo systemctl restart kafka-producer`.

### Tuning generator parameters for current resources

The bottleneck is **not** Kafka (1 broker handles a few thousand small msg/s comfortably) but
**Spark on a 2 vCPU node** (driver + 1 executor) and the **EC2 t3.micro generator** (burstable,
baseline ~10% CPU). Recommended:

| Parameter | Value | Reason |
|---|---|---|
| `--rate` | **5–8** ship/s (≈75–120 msg/s) | Fits within 1 Spark executor; service default = 6. |
| `--time-scale` | **2000** (default) | Each shipment ~100s wall-time → in-flight heap ~rate×100×15 events (~9k @ rate 6 ≈ 30 MB, fits the t3.micro's 1 GB RAM). |
| `--late-pct` | **0.05** | Matches the simulation rule, enough to exercise watermark behavior. |

```bash
# Quick dry-run (no Kafka needed) to estimate throughput:
cd /opt/simulation && python3 event_generator.py --dry-run --rate 6 --duration 20 | tail -5

# To change the service rate: edit GENERATOR_RATE in /opt/kafka-producer.env then restart,
# or set var.generator_rate in Terraform.
```

> If the generator is CPU-throttled (sent/s gradually dropping, t3.micro out of burst credits):
> lower `--rate` to 3–4, or switch `generator_instance_type` to `t3.small` (2 GiB, 2 vCPU).
> To push higher load (testing watermark/partitioning), first raise `spark`'s `desired_size`
> + `executor.instances`, then raise `--rate`.

---

## Quick Destroy / Recreate

### Destroy (in dependency order — REQUIRED)



```bash
cd infra/terraform

terraform destroy -target=module.helm_releases

terraform destroy -target=module.eks

terraform destroy \
  -target=module.s3 \
  -target=module.vpc \
  -target=module.security_groups \
  -target=module.iam \
  -target=module.ecr \
  -target=module.irsa

terraform state list
```

#### Common errors during destroy

1. **`DependencyViolation: Network vpc-... has some mapped public address(es)`** when deleting the Internet Gateway (hangs ~20 min then times out)
   - Cause: EKS nodes are still running (still have a public IP in the public subnet) because the VPC/IGW is destroyed before/concurrently with the node group.
   - Fix: run **Phase 2 (`-target=module.eks`) before Phase 3**. If already stuck mid-way, just run `terraform destroy -target=module.eks` to release the ENIs, then continue with Phase 3.

2. **`Failed to construct REST client: no client config`** (`module.helm_releases.kubernetes_manifest.kafka_node_pool`)
   - Cause: the cluster was already deleted but the config still has a `kubernetes_manifest` resource; this resource type needs to connect to the cluster at plan-time, even during destroy.
   - Fix: in Phase 3, use `-target` on the AWS modules instead of a bare destroy (this resource already left state along with Phase 1/2, so it no longer needs deleting).

3. **`Hook pre-delete ... jobs.batch "spark-operator-webhook-cleanup" already exists`** in Phase 1
   - Cause: a previous destroy was interrupted, leaving behind the Helm hook's cleanup Job.
   - Fix: `kubectl delete job spark-operator-webhook-cleanup -n spark-operator --ignore-not-found`, then re-run Phase 1.
   - If the Helm hook / Strimzi finalizer keeps hanging (the cluster will be fully deleted anyway so a "clean" uninstall isn't needed): remove the in-cluster resource straight from state and delete the cluster:
     ```bash
     terraform state list | grep "module.helm_releases" | xargs -n1 terraform state rm
     terraform destroy -target=module.eks   # continue Phase 2 → Phase 3
     ```

### Recreate from scratch

```bash
cd infra/terraform

terraform apply -target=module.vpc -target=module.security_groups \
  -target=module.iam -target=module.eks -target=module.s3 \
  -target=module.ecr -target=module.irsa

$(terraform output -raw kubeconfig_command)
terraform apply
```

---

## Common Troubleshooting

### Kafka broker won't start
```bash
kubectl describe kafka logistics-kafka -n kafka
kubectl logs -l strimzi.io/cluster=logistics-kafka -n kafka --tail=50
# Usually caused by insufficient node memory — check whether the node group has enough capacity
kubectl describe nodes | grep -A5 "Allocated resources"
```

### Spark can't read from Kafka
```bash
# Check Kafka bootstrap
kubectl run kafka-test --image=apache/kafka:3.7.0 --restart=Never -n kafka \
  -- kafka-topics.sh --bootstrap-server logistics-kafka-kafka-bootstrap.kafka.svc:9092 --list
kubectl logs kafka-test -n kafka
kubectl delete pod kafka-test -n kafka
```

### ClickHouse schema isn't created
```bash
# Re-run the init job
kubectl delete job clickhouse-schema-init -n clickhouse
# Terraform will recreate it on the next apply
```

### EC2 generator can't send events
```bash
# Get the node IP again (nodes may have been replaced due to Spot)
aws ec2 describe-instances \
  --filters "Name=tag:eks:cluster-name,Values=vdt-logistics-dev-eks" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].PrivateIpAddress" \
  --output table

# Update the bootstrap address in EC2 then restart the service
aws ssm start-session --target INSTANCE_ID
# sudo systemctl restart kafka-producer
```

### Spot node interrupted
No action needed — EKS automatically provisions a new node, and Spark Operator automatically restarts the job. Checkpoints guarantee no streaming state is lost.

---

## Spark Config for Iceberg (reference for writing jobs)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("logistics-streaming") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue.warehouse",
            f"s3a://{ICEBERG_BUCKET}/warehouse/") \
    .config("spark.sql.catalog.glue.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# Write to Bronze
df.writeTo("glue.vdt_logistics_dev_bronze.tracking_events") \
  .using("iceberg") \
  .tableProperty("write.format.default", "parquet") \
  .createOrReplace()
```

## ClickHouse Sink from Spark

```python
# Write KPI aggregates to ClickHouse
kpi_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/{CLICKHOUSE_DB}") \
    .option("dbtable", "kpi_by_post_office") \
    .option("user", CLICKHOUSE_USER) \
    .option("password", CLICKHOUSE_PASS) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .mode("append") \
    .save()
```
