# ── EKS Cluster SG ───────────────────────────────────────────────────────────

resource "aws_security_group" "eks_cluster" {
  name        = "${var.name_prefix}-eks-cluster-sg"
  description = "EKS control plane"
  vpc_id      = var.vpc_id
  tags        = { Name = "${var.name_prefix}-eks-cluster-sg" }
}

resource "aws_security_group_rule" "cluster_ingress_nodes" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_cluster.id
  source_security_group_id = aws_security_group.eks_node.id
  description              = "Nodes to API server"
}

resource "aws_security_group_rule" "cluster_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.eks_cluster.id
  cidr_blocks       = ["0.0.0.0/0"]
}

# ── EKS Node SG ──────────────────────────────────────────────────────────────

resource "aws_security_group" "eks_node" {
  name        = "${var.name_prefix}-eks-node-sg"
  description = "EKS worker nodes"
  vpc_id      = var.vpc_id
  tags = {
    Name                                        = "${var.name_prefix}-eks-node-sg"
    "kubernetes.io/cluster/${var.name_prefix}-eks" = "owned"
  }
}

resource "aws_security_group_rule" "node_ingress_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "-1"
  security_group_id        = aws_security_group.eks_node.id
  source_security_group_id = aws_security_group.eks_node.id
  description              = "Inter-node traffic"
}

resource "aws_security_group_rule" "node_ingress_cluster" {
  type                     = "ingress"
  from_port                = 1025
  to_port                  = 65535
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_node.id
  source_security_group_id = aws_security_group.eks_cluster.id
  description              = "Control plane to node kubelet/ports"
}

resource "aws_security_group_rule" "node_ingress_cluster_443" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_node.id
  source_security_group_id = aws_security_group.eks_cluster.id
  description              = "Control plane webhooks"
}

# Kafka NodePort — data generator EC2 → EKS nodes
resource "aws_security_group_rule" "node_ingress_kafka_nodeport" {
  type                     = "ingress"
  from_port                = 32092
  to_port                  = 32092
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_node.id
  source_security_group_id = aws_security_group.generator.id
  description              = "Kafka NodePort from data generator"
}

resource "aws_security_group_rule" "node_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.eks_node.id
  cidr_blocks       = ["0.0.0.0/0"]
}


# ── Data Generator EC2 SG ─────────────────────────────────────────────────────

resource "aws_security_group" "generator" {
  name        = "${var.name_prefix}-generator-sg"
  description = "Python data generator EC2"
  vpc_id      = var.vpc_id
  tags        = { Name = "${var.name_prefix}-generator-sg" }
}

resource "aws_security_group_rule" "generator_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.generator.id
  cidr_blocks       = ["0.0.0.0/0"]
  description       = "All outbound (Kafka, SSM, S3)"
}
