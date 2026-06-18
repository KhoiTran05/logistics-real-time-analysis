# Hướng Dẫn Triển Khai — VDT Logistics Realtime Pipeline (AWS, Student Demo)

## Kiến Trúc Tổng Quan

```
EC2 t3.micro (Python generator)
         │  NodePort 32092
         ▼
Kafka (Strimzi/KRaft, 1 broker, trên EKS)
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

ClickHouse (trên EKS, serving layer)
         │
Grafana (trên EKS, ClickHouse datasource)

Airflow (trên EKS, trigger batch Spark jobs)
```

## Demo Compromises 

| Thành phần | Production | Demo |
|---|---|---|
| Kafka | MSK multi-broker HA | Strimzi 1 broker trên EKS — tiết kiệm ~$100/tháng |
| EKS nodes | On-Demand 5 groups | general 2–4× m7i-flex.large + spark 1–5× m7i-flex.large, On-Demand, free-tier cap; **Cluster Autoscaler** tự scale theo pod Pending (chạy ~3-4h/ngày nên để max rộng cho thoải mái) |
| Availability | Multi-AZ | Single AZ — tiết kiệm ~$32/tháng (NAT GW) |
| Kafka auth | TLS + SASL | Plain (no auth, private VPC) |
| Airflow | CeleryExecutor + Redis | LocalExecutor (tasks sequential) |
| Kafka partitions | 64/32/16 | 8/4/2 (đủ cho 200 msg/s demo) |

---

## Ước Tính Chi Phí

| Dịch vụ | Cấu hình | USD/tháng |
|---|---|---|
| EKS Control Plane | 1 cluster | $73 (24/7) · ~$12 (3-4h/ngày) |
| EC2 — general (Kafka/Airflow/Grafana/CH) | 2–4× m7i-flex.large On-Demand | ~$155/tháng (24/7) · **~$20 (3-4h/ngày)** |
| EC2 — spark (autoscaled) | 1–5× m7i-flex.large On-Demand | ~$8/tháng (3-4h/ngày, scale-to-min khi idle) |
| EC2 t3.micro (generator) | 1× On-Demand | $8 |
| NAT Gateway | 1× | $32 |
| S3 | ~200 GB | $5 |
| Glue Catalog | 3 databases | <$1 |
| **Tổng (24/7)** | | **~$260/tháng** |
| **Tổng (destroy/recreate ~3-4h/ngày)** | | **~$65–75/tháng** |

> **Chế độ ~3-4h/ngày (khuyến nghị):** `terraform destroy` sau mỗi phiên → gần như mọi chi phí đều là biến phí (EKS control plane, EC2 general+spark, NAT GW chỉ tính giờ chạy). Cluster Autoscaler tự thu nhỏ pool spark về `min_size` khi không có job, nên không trả tiền cho node spark rảnh. Chỉ S3 (~$5) là cố định.

---

## Yêu Cầu Tiên Quyết

```bash
# Kiểm tra các công cụ cần thiết
terraform version     
aws --version         # >= 2.x
kubectl version --client
helm version          # >= 3.12
```

Credentials AWS:
```bash
aws configure
# Hoặc export AWS_PROFILE=my-profile
aws sts get-caller-identity
```

---

## Bước 1 — Bootstrap Terraform State Backend

```bash
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="ap-southeast-1"

# S3 bucket cho state
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

# Tạo backend.tfvars
cat > infra/terraform/backend.tfvars << EOF
bucket         = "vdt-terraform-state-${AWS_ACCOUNT_ID}"
key            = "vdt-mini-project/terraform.tfstate"
region         = "ap-southeast-1"
encrypt        = true
use_lockfile   = true
EOF
```

---

## Bước 2 — Apply Phase 1: Core Infrastructure

Do Terraform không thể configure Kubernetes/Helm provider trước khi EKS cluster tồn tại, cần apply 2 phase.

```bash
cd infra/terraform

# Khởi tạo Terraform
terraform init -backend-config=backend.tfvars

# Xem plan
terraform plan \
  -target=module.vpc \
  -target=module.security_groups \
  -target=module.iam \
  -target=module.eks \
  -target=module.s3 \
  -target=module.ecr \
  -target=module.irsa

# Phase 1: Tạo VPC, EKS, IAM, S3 (~15-20 phút)
terraform apply \
  -target=module.vpc \
  -target=module.security_groups \
  -target=module.iam \
  -target=module.eks \
  -target=module.s3 \
  -target=module.ecr \
  -target=module.irsa

# Sau khi Phase 1 xong, cập nhật kubeconfig
$(terraform output -raw kubeconfig_command)
kubectl get nodes
```

---

## Bước 3 — Apply Phase 2: Platform (Helm + Kafka + ClickHouse + Grafana)

```bash
cd infra/terraform

# Phase 2: Deploy toàn bộ platform (~20-30 phút)
# Thứ tự tự động: Strimzi → Kafka cluster → Topics → Airflow → ClickHouse → Schema → Grafana
terraform apply -target=module.helm_releases.helm_release.strimzi

terraform apply

# Kiểm tra tất cả pods
kubectl get pods -A

# Kiểm tra Kafka đang chạy
kubectl get kafka -n kafka
kubectl get kafkatopic -n kafka

# Kiểm tra ClickHouse
kubectl get pods -n clickhouse

# Kiểm tra Grafana
kubectl get pods -n monitoring
```

---

## Bước 4 — Build và Push Spark Image

```bash
# Upload Spark scripts lên S3
ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)
aws s3 sync src/ "s3://${ARTIFACTS}/src/" --exclude "__pycache__/*"
```

---

## Bước 4b — Seed Dim Tables + Upload Generator Scripts

`simulation/catalog.py` là **nguồn ID duy nhất**: cùng dữ liệu được seeder ghi vào Dim tables
và được event generator dùng để sinh event → mọi event tham chiếu đúng ID có thật trong Dim
(xem [streaming_events_schema.md](streaming_events_schema.md)).

```bash
ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)

pip install -r simulation/requirements.txt

# Seed Dim tables (chạy MỘT lần) -> s3://${ARTIFACTS}/dim-seed/<table>/<table>.csv
python simulation/dim_seeder.py --s3-bucket "${ARTIFACTS}"

#
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

# Upload scripts cho EC2 generator (EC2 tự sync s3://.../simulation/ ở mỗi lần start)
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

> EC2 generator boot trong Phase 2 (Bước 3) **trước** khi scripts có trên S3 — không sao:
> service tự fail và systemd retry mỗi 30s, đến khi `aws s3 sync` ở trên hoàn tất nó sẽ tự chạy.

---

## Bước 5 — Submit Spark Streaming Job

```bash
ICEBERG_BUCKET=$(cd infra/terraform && terraform output -raw s3_iceberg_bucket)
CHECKPOINTS_BUCKET=$(cd infra/terraform && terraform output -raw s3_checkpoints_bucket)
ARTIFACTS=$(cd infra/terraform && terraform output -raw s3_artifacts_bucket)
GLUE_DBS=$(cd infra/terraform && terraform output -json glue_databases)

cat << EOF | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: logistics-streaming
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: ${SPARK_IMAGE}
  imagePullPolicy: Always
  mainApplicationFile: "s3a://${ARTIFACTS}/spark-scripts/streaming/main.py"
  sparkVersion: "3.5.1"

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 30

  # Spark pool = 2 vCPU / 8 GiB nodes (1 pod/node). Cluster Autoscaler scales the
  # spark pool up to max_size when these pods are Pending, so driver + 2 executors
  # land on 3 separate nodes automatically; the pool scales back down when the job
  # ends. For heavier batch, raise executor.instances and the spark max_size.
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    nodeSelector:
      node-type: spark
    tolerations:
      - key: dedicated
        value: spark
        effect: NoSchedule
    envVars:
      - name: KAFKA_BOOTSTRAP
        value: "logistics-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
      - name: ICEBERG_BUCKET
        value: "${ICEBERG_BUCKET}"
      - name: CHECKPOINTS_BUCKET
        value: "${CHECKPOINTS}"
      - name: CLICKHOUSE_HOST
        value: "clickhouse.clickhouse.svc.cluster.local"
      - name: CLICKHOUSE_PORT
        value: "8123"

  executor:
    cores: 1
    instances: 2
    memory: "2g"
    nodeSelector:
      node-type: spark
    tolerations:
      - key: dedicated
        value: spark
        effect: NoSchedule

  sparkConf:
    # Iceberg + Glue Catalog
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog"
    "spark.sql.catalog.glue.warehouse": "s3a://${ICEBERG_BUCKET}/warehouse/"
    "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    # AWS credentials via IRSA
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    # Streaming checkpoint
    "spark.streaming.stopGracefullyOnShutdown": "true"
EOF

# Monitor
kubectl get sparkapplication -n spark
kubectl logs -l spark-role=driver -n spark -f
```

---

## Bước 6 — Truy Cập Dashboard

```bash
# Port-forward Grafana
kubectl port-forward svc/grafana 3000:80 -n monitoring &
# Truy cập: http://localhost:3000
# Credentials:
aws secretsmanager get-secret-value \
  --secret-id $(cd infra/terraform && terraform output -raw grafana_password_secret) \
  --query SecretString --output text | python3 -m json.tool

# Port-forward Airflow
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow &
# Truy cập: http://localhost:8080 (admin/admin mặc định)

# Xem ClickHouse trực tiếp
kubectl exec -it -n clickhouse \
  $(kubectl get pod -n clickhouse -l app.kubernetes.io/name=clickhouse -o jsonpath='{.items[0].metadata.name}') \
  -- clickhouse-client --user admin --database logistics
```

---

## Bước 7 — Kiểm Tra Generator EC2

```bash
GENERATOR_ID=$(cd infra/terraform && terraform output -raw generator_instance_id)

# Kết nối via SSM (không cần SSH key)
aws ssm start-session --target "${GENERATOR_ID}" --region ap-southeast-1

# Bên trong EC2:
cat /opt/kafka-producer.env
ls /opt/simulation                 # catalog.py, event_generator.py, ...
systemctl status kafka-producer
journalctl -u kafka-producer -f    # "[Ns] created=… sent=… (~N msg/s)"
```

> Nếu generator chưa gửi event: kiểm tra scripts đã có trên S3 chưa
> (`aws s3 ls s3://${ARTIFACTS}/simulation/`) rồi `sudo systemctl restart kafka-producer`.

### Tuning tham số generator cho tài nguyên hiện tại

Bottleneck **không phải** Kafka (1 broker xử lý vài nghìn msg nhỏ/s thoải mái) mà là
**Spark trên node 2 vCPU** (driver + 1 executor) và **EC2 generator t3.micro** (burstable,
baseline ~10% CPU). Khuyến nghị:

| Tham số | Giá trị | Lý do |
|---|---|---|
| `--rate` | **5–8** ship/s (≈75–120 msg/s) | Vừa với 1 executor Spark; mặc định service = 6. |
| `--time-scale` | **2000** (mặc định) | Mỗi shipment ~100s wall-time → heap in-flight ~rate×100×15 events (~9k @ rate 6 ≈ 30 MB, vừa RAM 1 GB của t3.micro). |
| `--late-pct` | **0.05** | Đúng theo simulation rule, đủ để test watermark. |

```bash
# Chạy thử nhẹ (dry-run, không cần Kafka) để ước lượng throughput:
cd /opt/simulation && python3 event_generator.py --dry-run --rate 6 --duration 20 | tail -5

# Đổi rate cho service: sửa GENERATOR_RATE trong /opt/kafka-producer.env rồi restart,
# hoặc set var.generator_rate trong Terraform.
```

> Nếu generator bị throttle CPU (sent/s tụt dần, t3.micro hết credit): hạ `--rate`
> xuống 3–4, hoặc đổi `generator_instance_type` sang `t3.small` (2 GiB, 2 vCPU).
> Muốn đẩy tải cao hơn (test watermark/partition) thì tăng `spark` `desired_size`
> + `executor.instances` trước, rồi mới tăng `--rate`.

---

## Quick Destroy / Recreate

```bash
cd infra/terraform

# Destroy hoàn toàn (~10 phút)
terraform destroy

# Recreate từ đầu
terraform apply -target=module.vpc -target=module.security_groups \
  -target=module.iam -target=module.eks -target=module.s3 \
  -target=module.ecr -target=module.irsa

$(terraform output -raw kubeconfig_command)
terraform apply
```

---

## Xử Lý Sự Cố Thường Gặp

### Kafka broker không start
```bash
kubectl describe kafka logistics-kafka -n kafka
kubectl logs -l strimzi.io/cluster=logistics-kafka -n kafka --tail=50
# Thường do node không đủ memory — kiểm tra node group có đủ capacity
kubectl describe nodes | grep -A5 "Allocated resources"
```

### Spark không đọc được Kafka
```bash
# Kiểm tra Kafka bootstrap
kubectl run kafka-test --image=apache/kafka:3.7.0 --restart=Never -n kafka \
  -- kafka-topics.sh --bootstrap-server logistics-kafka-kafka-bootstrap.kafka.svc:9092 --list
kubectl logs kafka-test -n kafka
kubectl delete pod kafka-test -n kafka
```

### ClickHouse schema không được tạo
```bash
# Chạy lại job init
kubectl delete job clickhouse-schema-init -n clickhouse
# Terraform sẽ tạo lại khi apply
```

### EC2 generator không gửi được event
```bash
# Lấy lại node IP (nodes có thể bị replace do Spot)
aws ec2 describe-instances \
  --filters "Name=tag:eks:cluster-name,Values=vdt-logistics-dev-eks" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].PrivateIpAddress" \
  --output table

# Update bootstrap trong EC2 rồi restart service
aws ssm start-session --target INSTANCE_ID
# sudo systemctl restart kafka-producer
```

### Spot node bị interrupt
Không cần can thiệp — EKS tự provision node mới, Spark Operator tự restart job. Checkpoint đảm bảo không mất state streaming.

---

## Spark Config cho Iceberg (tham khảo khi viết job)

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

# Viết vào Bronze
df.writeTo("glue.vdt_logistics_dev_bronze.tracking_events") \
  .using("iceberg") \
  .tableProperty("write.format.default", "parquet") \
  .createOrReplace()
```

## ClickHouse Sink từ Spark

```python
# Ghi KPI aggregates vào ClickHouse
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
