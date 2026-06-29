#!/bin/bash
set -euo pipefail

# Install dependencies
dnf update -y
dnf install -y python3 python3-pip git awscli amazon-ssm-agent

# Ensure SSM Agent is running (absent on al2023-ami-minimal) so Session Manager works
systemctl enable --now amazon-ssm-agent

pip3 install kafka-python boto3 lz4

# Discover Kafka bootstrap via EKS node IPs (NodePort)
discover_kafka_bootstrap() {
  NODE_IP=$(aws ec2 describe-instances \
    --region "${aws_region}" \
    --filters \
      "Name=tag:eks:cluster-name,Values=${eks_cluster_name}" \
      "Name=tag:eks:nodegroup-name,Values=*general*" \
      "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].PrivateIpAddress" \
    --output text)

  if [[ "$NODE_IP" == "None" || -z "$NODE_IP" ]]; then
    # Fallback: any running EKS node
    NODE_IP=$(aws ec2 describe-instances \
      --region "${aws_region}" \
      --filters \
        "Name=tag:eks:cluster-name,Values=${eks_cluster_name}" \
        "Name=instance-state-name,Values=running" \
      --query "Reservations[0].Instances[0].PrivateIpAddress" \
      --output text)
  fi

  echo "$${NODE_IP}:${kafka_nodeport}"
}

KAFKA_BOOTSTRAP=$(discover_kafka_bootstrap)

mkdir -p /opt/simulation

# Write environment config
cat > /opt/kafka-producer.env << EOF
KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP
AWS_REGION=${aws_region}
EKS_CLUSTER=${eks_cluster_name}
KAFKA_NODEPORT=${kafka_nodeport}
GENERATOR_RATE=${generator_rate}
EOF

# Write systemd service — starts automatically on boot
cat > /etc/systemd/system/kafka-producer.service << 'SVCEOF'
[Unit]
Description=VDT Logistics Kafka Data Generator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/simulation
EnvironmentFile=/opt/kafka-producer.env
# Pull simulation scripts on every start — same files used to seed the Dim tables,
# so event IDs match Dim rows. Self-heals: if scripts aren't uploaded yet the unit
# fails and systemd retries (RestartSec) until the s3 sync succeeds.
ExecStartPre=/usr/bin/aws s3 sync s3://${artifacts_bucket}/simulation/ /opt/simulation/ --region ${aws_region}
ExecStartPre=/bin/bash -c 'test -f /opt/simulation/event_generator.py'
# Re-discover Kafka bootstrap if the cached one is unreachable (Spot nodes get replaced)
ExecStartPre=/bin/bash -c 'KAFKA_BOOTSTRAP=$(. /opt/kafka-producer.env; echo $KAFKA_BOOTSTRAP); \
  if ! python3 -c "from kafka import KafkaProducer; KafkaProducer(bootstrap_servers=\"$KAFKA_BOOTSTRAP\")" 2>/dev/null; then \
    NODE_IP=$(aws ec2 describe-instances \
      --region $AWS_REGION \
      --filters "Name=tag:eks:cluster-name,Values=$EKS_CLUSTER" \
               "Name=instance-state-name,Values=running" \
      --query "Reservations[0].Instances[0].PrivateIpAddress" --output text); \
    sed -i "s|^KAFKA_BOOTSTRAP=.*|KAFKA_BOOTSTRAP=$NODE_IP:$KAFKA_NODEPORT|" /opt/kafka-producer.env; \
  fi'
ExecStart=/usr/bin/python3 /opt/simulation/event_generator.py --rate ${generator_rate} --duration 600
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable kafka-producer
systemctl start kafka-producer

echo "Data generator setup complete. Bootstrap: $KAFKA_BOOTSTRAP"
