resource "random_password" "clickhouse" {
  length  = 20
  special = false
}

resource "random_password" "grafana" {
  length  = 20
  special = false
}

resource "aws_secretsmanager_secret" "clickhouse" {
  name                    = "${var.name_prefix}/clickhouse"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "clickhouse" {
  secret_id     = aws_secretsmanager_secret.clickhouse.id
  secret_string = jsonencode({ username = var.clickhouse_username, password = random_password.clickhouse.result })
}

resource "aws_secretsmanager_secret" "grafana" {
  name                    = "${var.name_prefix}/grafana"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "grafana" {
  secret_id     = aws_secretsmanager_secret.grafana.id
  secret_string = jsonencode({ username = "admin", password = random_password.grafana.result })
}

# ── Namespaces ────────────────────────────────────────────────────────────────

resource "kubernetes_namespace" "namespaces" {
  for_each = toset(["spark", "spark-operator", "kafka", "airflow", "clickhouse", "monitoring"])

  metadata { name = each.key }
}

# ── Default StorageClass ──────────────────────────────────────────────────────
# EKS creates the gp2 StorageClass but does not mark it default. PVCs that don't
# name a class (Airflow, Kafka broker) stay Pending forever without a default.
# force = true to override the field manager owned by the EKS control plane.

resource "kubernetes_annotations" "gp2_default" {
  api_version = "storage.k8s.io/v1"
  kind        = "StorageClass"
  metadata { name = "gp2" }
  annotations = {
    "storageclass.kubernetes.io/is-default-class" = "true"
  }
  force = true
}

# ── Spark ServiceAccount (IRSA) ───────────────────────────────────────────────

resource "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = "spark"
    annotations = {
      "eks.amazonaws.com/role-arn" = var.spark_irsa_role_arn
    }
  }
  depends_on = [kubernetes_namespace.namespaces]
}

resource "kubernetes_role" "spark_driver" {
  metadata {
    name      = "spark-driver-role"
    namespace = "spark"
  }
  rule {
    api_groups = [""]
    resources  = ["pods", "services", "configmaps", "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"]
  }
  depends_on = [kubernetes_namespace.namespaces]
}

resource "kubernetes_role_binding" "spark_driver" {
  metadata {
    name      = "spark-driver-rolebinding"
    namespace = "spark"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.spark_driver.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark.metadata[0].name
    namespace = "spark"
  }
}

# ── Spark Operator ────────────────────────────────────────────────────────────

resource "helm_release" "spark_operator" {
  name             = "spark-operator"
  repository       = "https://kubeflow.github.io/spark-operator"
  chart            = "spark-operator"
  version          = "1.2.15"
  namespace        = "spark-operator"
  create_namespace = false
  timeout          = 300

  # Custom operator image with hadoop-aws baked in, so the operator's in-process
  # spark-submit can resolve the s3a:// mainApplicationFile / pyFiles.
  set {
    name  = "image.repository"
    value = var.spark_operator_image_repository
  }
  set {
    name  = "image.tag"
    value = var.spark_operator_image_tag
  }

  set {
    name  = "sparkJobNamespace"
    value = "spark"
  }
  set {
    name  = "webhook.enable"
    value = "true"
  }
  set {
    name  = "serviceAccounts.spark.create"
    value = "false"
  }
  set {
    name  = "serviceAccounts.spark.name"
    value = "spark"
  }
  # IRSA for the operator SA: grants the spark-submit subprocess S3 read access to
  # the artifacts bucket (reuses the spark role, which already has ArtifactsRead).
  set {
    name  = "serviceAccounts.sparkoperator.annotations.eks\\.amazonaws\\.com/role-arn"
    value = var.spark_irsa_role_arn
  }

  depends_on = [kubernetes_service_account.spark]
}

# ── Cluster Autoscaler ────────────────────────────────────────────────────────
# Scales the managed node groups up when pods are Pending (e.g. Spark executors)
# and back down when idle — important since the cluster runs only a few hours/day.

resource "helm_release" "cluster_autoscaler" {
  name             = "cluster-autoscaler"
  repository       = "https://kubernetes.github.io/autoscaler"
  chart            = "cluster-autoscaler"
  version          = "9.37.0"
  namespace        = "kube-system"
  create_namespace = false
  timeout          = 300

  values = [yamlencode({
    autoDiscovery = { clusterName = var.cluster_name }
    awsRegion     = var.aws_region

    # Match the cluster minor version (k8s 1.30)
    image = { tag = "v1.30.1" }

    rbac = {
      serviceAccount = {
        name        = "cluster-autoscaler"
        annotations = { "eks.amazonaws.com/role-arn" = var.cluster_autoscaler_irsa_role_arn }
      }
    }

    extraArgs = {
      "balance-similar-node-groups" = "true"
      "skip-nodes-with-system-pods" = "false"
      "scale-down-unneeded-time"    = "5m"
      "scale-down-delay-after-add"  = "5m"
      "expander"                    = "least-waste"
    }

    # Autoscaler itself runs on the stable general pool
    nodeSelector = { "node-type" = "general" }
    resources = {
      requests = { cpu = "100m", memory = "300Mi" }
      limits   = { cpu = "200m", memory = "500Mi" }
    }
  })]

  depends_on = [kubernetes_namespace.namespaces]
}

# ── Strimzi Kafka Operator ────────────────────────────────────────────────────

resource "helm_release" "strimzi" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = "0.40.0"
  namespace        = "kafka"
  create_namespace = false
  timeout          = 300

  # Operator is installed into the "kafka" namespace and watches that same
  # namespace. watchNamespaces must list only ADDITIONAL namespaces — including
  # the release namespace here makes the chart render duplicate RoleBindings
  # ("already exists" on install). Empty = watch own namespace (kafka).

  depends_on = [kubernetes_namespace.namespaces]
}

# Wait for Strimzi CRDs to become available before creating CRs
resource "time_sleep" "wait_strimzi" {
  depends_on      = [helm_release.strimzi]
  create_duration = "45s"
}

# ── Kafka Cluster (KRaft, single broker — demo compromise) ────────────────────

resource "kubernetes_manifest" "kafka_node_pool" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaNodePool"
    metadata = {
      name      = "combined"
      namespace = "kafka"
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      replicas = 1
      roles    = ["controller", "broker"]
      storage = {
        type = "jbod"
        volumes = [{
          id          = 0
          type        = "persistent-claim"
          size        = var.kafka_storage_size
          deleteClaim = true
        }]
      }
      resources = {
        requests = { memory = "1Gi", cpu = "500m" }
        limits   = { memory = "2Gi", cpu = "1" }
      }
    }
  }
  depends_on = [time_sleep.wait_strimzi, kubernetes_annotations.gp2_default]
}

resource "kubernetes_manifest" "kafka_cluster" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "Kafka"
    metadata = {
      name      = var.kafka_cluster_name
      namespace = "kafka"
      annotations = {
        "strimzi.io/node-pools" = "enabled"
        "strimzi.io/kraft"      = "enabled"
      }
    }
    spec = {
      kafka = {
        version         = var.kafka_version
        metadataVersion = "3.7-IV4"
        listeners = [
          # Internal listener — Spark pods in EKS
          {
            name = "plain"
            port = 9092
            type = "internal"
            tls  = false
          },
          # NodePort listener — EC2 data generator (same VPC)
          {
            name = "external"
            port = 9093
            type = "nodeport"
            tls  = false
            configuration = {
              bootstrap = { nodePort = 32092 }
            }
          }
        ]
        config = {
          "offsets.topic.replication.factor"         = "1"
          "transaction.state.log.replication.factor" = "1"
          "transaction.state.log.min.isr"            = "1"
          "default.replication.factor"               = "1"
          "min.insync.replicas"                      = "1"
          "auto.create.topics.enable"                = "false"
          "num.partitions"                           = "4"
          "log.retention.hours"                      = "168"
          "compression.type"                         = "lz4"
        }
      }
      entityOperator = {
        topicOperator = {}
        userOperator  = {}
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_node_pool]
}

# Give Kafka cluster time to become Ready before creating topics
resource "time_sleep" "wait_kafka" {
  depends_on      = [kubernetes_manifest.kafka_cluster]
  create_duration = "120s"
}

# ── Kafka Topics (via Strimzi KafkaTopic CRD) ─────────────────────────────────

locals {
  # Topic names + partition keys per docs/streaming_events_schema.md.
  # Partitions reduced from prod (64/32/16) to demo scale.
  kafka_topics = {
    "logistics.tracking.events" = {
      # Reduced from 64 to 8 — demo scale (500-2000 msg/s fine with 8 partitions)
      partitions = 8
      replicas   = 1
      config     = { "retention.ms" = "604800000", "compression.type" = "lz4" }
    }
    "logistics.shipment.events" = {
      partitions = 4
      replicas   = 1
      config     = { "retention.ms" = "604800000", "compression.type" = "lz4" }
    }
    "logistics.financial.events" = {
      partitions = 2
      replicas   = 1
      config     = { "retention.ms" = "604800000", "compression.type" = "lz4" }
    }
  }
}

resource "kubernetes_manifest" "kafka_topics" {
  for_each = local.kafka_topics

  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = each.key
      namespace = "kafka"
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = each.value.partitions
      replicas   = each.value.replicas
      config     = each.value.config
    }
  }
  depends_on = [time_sleep.wait_kafka]
}

# ── Airflow ───────────────────────────────────────────────────────────────────

resource "helm_release" "airflow" {
  name             = "airflow"
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = "1.13.1"
  namespace        = "airflow"
  create_namespace = false
  timeout          = 600

  values = [yamlencode({
    # LocalExecutor — simpler than KubernetesExecutor, sufficient for demo
    # Demo compromise: tasks run sequentially inside the scheduler pod
    executor = "LocalExecutor"

    # Run the DB-migration and user-creation jobs as normal resources, not Helm
    # post-install hooks. The Terraform helm provider installs with --wait, which
    # waits for the scheduler/webserver to be Ready; their wait-for-migrations init
    # container blocks on migrations that a post-install hook would only run *after*
    # those pods are Ready → deadlock. As plain resources they run immediately.
    migrateDatabaseJob = { useHelmHooks = false }
    createUserJob      = { useHelmHooks = false }

    scheduler = {
      replicas     = 1
      resources    = { requests = { cpu = "300m", memory = "512Mi" }, limits = { cpu = "1", memory = "1Gi" } }
      nodeSelector = { "node-type" = "general" }
    }

    webserver = {
      replicas     = 1
      resources    = { requests = { cpu = "200m", memory = "512Mi" }, limits = { cpu = "500m", memory = "1Gi" } }
      nodeSelector = { "node-type" = "general" }
    }

    # Bundled PostgreSQL as the metadata DB (demo). The chart's default image
    # tag (bitnami/postgresql:...-r15) was removed from Docker Hub by the Aug 2025
    # Bitnami catalog migration; point at bitnamilegacy with an existing tag.
    postgresql = {
      enabled = true
      image = {
        registry   = "docker.io"
        repository = "bitnamilegacy/postgresql"
        tag        = "16.1.0-debian-11-r25"
      }
    }

    triggerer = { enabled = false }
    flower    = { enabled = false }
    statsd    = { enabled = false }

    serviceAccount = {
      create      = true
      name        = "airflow"
      annotations = { "eks.amazonaws.com/role-arn" = var.airflow_irsa_role_arn }
    }

    dags = {
      persistence = { enabled = false }
      gitSync = {
        enabled = true
        repo    = var.dags_git_repo
        branch  = var.dags_git_branch
        subPath = var.dags_git_subpath
        depth   = 1
        wait    = 30
      }
    }

    logs = { persistence = { enabled = false } }

    env = [
      {
        name  = "AIRFLOW__CORE__LOAD_EXAMPLES"
        value = "false"
      },
      {
        name  = "AIRFLOW__CORE__PARALLELISM"
        value = "4"
      },
      {
        name  = "S3_ARTIFACTS_BUCKET"
        value = var.artifacts_bucket_name
      },
      {
        name  = "S3_ICEBERG_BUCKET"
        value = var.iceberg_bucket_name
      },
      {
        name  = "AWS_REGION"
        value = var.aws_region
      }
    ]
  })]

  depends_on = [kubernetes_annotations.gp2_default]
}

# ── ClickHouse ────────────────────────────────────────────────────────────────

resource "helm_release" "clickhouse" {
  name             = "clickhouse"
  repository       = "https://charts.bitnami.com/bitnami"
  chart            = "clickhouse"
  version          = "6.0.0"
  namespace        = "clickhouse"
  create_namespace = false
  timeout          = 300

  values = [yamlencode({
    shards       = 1
    replicaCount = 1

    # Bitnami relocated versioned images to the bitnamilegacy/ repo (Aug 2025);
    # the chart's default bitnami/clickhouse:<tag> no longer exists on Docker Hub.
    image = {
      registry   = "docker.io"
      repository = "bitnamilegacy/clickhouse"
      tag        = "24.3.2-debian-12-r0"
    }

    auth = {
      username = var.clickhouse_username
      password = random_password.clickhouse.result
      database = var.clickhouse_db
    }

    persistence = {
      enabled      = true
      size         = "20Gi"
      storageClass = "gp2"
    }

    resources = {
      requests = { memory = "1Gi", cpu = "500m" }
      limits   = { memory = "3Gi", cpu = "2" }
    }

    nodeSelector = { "node-type" = "general" }

    service = {
      type  = "ClusterIP"
      ports = { http = 8123, tcp = 9000 }
    }

    # Minimal replica settings for single-node demo
    keeper    = { enabled = false }
    zookeeper = { enabled = false }
  })]

  depends_on = [kubernetes_namespace.namespaces]
}

# Wait for ClickHouse to be ready before initialising schema
resource "time_sleep" "wait_clickhouse" {
  depends_on      = [helm_release.clickhouse]
  create_duration = "60s"
}

# Namespace-local copy of the ClickHouse credentials for the Spark driver to
# reach the serving layer (KPI JDBC sink). Same password the server runs with.
resource "kubernetes_secret" "clickhouse_spark" {
  metadata {
    name      = "clickhouse-credentials"
    namespace = "spark"
  }
  data = {
    username = var.clickhouse_username
    password = random_password.clickhouse.result
    database = var.clickhouse_db
  }
  depends_on = [kubernetes_namespace.namespaces]
}

# ── ClickHouse Schema Init ────────────────────────────────────────────────────

resource "kubernetes_job" "clickhouse_init" {
  metadata {
    name      = "clickhouse-schema-init"
    namespace = "clickhouse"
  }
  spec {
    backoff_limit = 3
    template {
      metadata {}
      spec {
        restart_policy = "OnFailure"
        container {
          name = "init"
          # Reuse the running server's image — the standalone clickhouse-client
          # image is deprecated and this tag is guaranteed to exist (server pulls it).
          image   = "docker.io/bitnamilegacy/clickhouse:24.3.2-debian-12-r0"
          command = ["/bin/sh", "-c"]
          args = [<<-EOT
            clickhouse-client \
              --host clickhouse.clickhouse.svc.cluster.local \
              --port 9000 \
              --user ${var.clickhouse_username} \
              --password ${random_password.clickhouse.result} \
              --multiquery "
                CREATE DATABASE IF NOT EXISTS ${var.clickhouse_db};

                -- Operational financial KPIs (§1): revenue + COD + COD success-rate
                -- components, by facility, sliding window. rate = collected/committed.
                CREATE TABLE IF NOT EXISTS ${var.clickhouse_db}.kpi_financial (
                  window_start        DateTime,
                  window_end          DateTime,
                  facility_id         String,
                  total_revenue_vnd   Int64,
                  total_cod_vnd       Int64,
                  cod_collected_count UInt64,
                  cod_committed_count UInt64
                ) ENGINE = SummingMergeTree
                ORDER BY (window_start, window_end, facility_id);

                -- Order volume by pickup facility (§1), tumbling window.
                CREATE TABLE IF NOT EXISTS ${var.clickhouse_db}.kpi_order_volume_facility (
                  window_start       DateTime,
                  window_end         DateTime,
                  pickup_facility_id String,
                  order_count        UInt64
                ) ENGINE = SummingMergeTree
                ORDER BY (window_start, window_end, pickup_facility_id);

                -- Order volume by partner / service (§1), tumbling window.
                CREATE TABLE IF NOT EXISTS ${var.clickhouse_db}.kpi_order_volume_partner_service (
                  window_start    DateTime,
                  window_end      DateTime,
                  partner_id      String,
                  service_type_id String,
                  order_count     UInt64
                ) ENGINE = SummingMergeTree
                ORDER BY (window_start, window_end, partner_id, service_type_id);

                CREATE TABLE IF NOT EXISTS ${var.clickhouse_db}.anomaly_alerts (
                  detected_at      DateTime,
                  shipment_id      String,
                  facility_id      String,
                  anomaly_type     String,
                  severity         String,
                  detail           String
                ) ENGINE = MergeTree()
                ORDER BY (detected_at, anomaly_type);
              "
          EOT
          ]
        }
      }
    }
  }
  wait_for_completion = true
  depends_on          = [time_sleep.wait_clickhouse]
}

# ── Grafana ───────────────────────────────────────────────────────────────────

resource "helm_release" "grafana" {
  name             = "grafana"
  repository       = "https://grafana.github.io/helm-charts"
  chart            = "grafana"
  version          = "7.3.9"
  namespace        = "monitoring"
  create_namespace = false
  timeout          = 300

  values = [yamlencode({
    adminPassword = random_password.grafana.result

    # Install ClickHouse datasource plugin
    plugins = ["grafana-clickhouse-datasource"]

    datasources = {
      "datasources.yaml" = {
        apiVersion = 1
        datasources = [{
          name      = "ClickHouse"
          type      = "grafana-clickhouse-datasource"
          isDefault = true
          jsonData = {
            host            = "clickhouse.clickhouse.svc.cluster.local"
            port            = 9000
            username        = var.clickhouse_username
            defaultDatabase = var.clickhouse_db
            protocol        = "native"
          }
          secureJsonData = {
            password = random_password.clickhouse.result
          }
        }]
      }
    }

    resources = {
      requests = { memory = "256Mi", cpu = "100m" }
      limits   = { memory = "512Mi", cpu = "500m" }
    }

    nodeSelector = { "node-type" = "general" }

    persistence = {
      enabled      = true
      size         = "5Gi"
      storageClass = "gp2"
    }

    service = { type = "ClusterIP" }

    grafana_ini = {
      server = {
        root_url            = "%(protocol)s://%(domain)s:%(http_port)s/grafana"
        serve_from_sub_path = true
      }
    }
  })]

  depends_on = [kubernetes_job.clickhouse_init]
}
