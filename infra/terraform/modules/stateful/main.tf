variable "name" {
  type = string
}
variable "region" {
  type = string
}
variable "account_id" {
  type = string
}
variable "az" {
  type = string
}
variable "subnet_id" {
  type = string
}
variable "groups" {
  type = map(string)
}
variable "secrets" {
  type = map(string)
}
variable "images" {
  type = map(string)
}
variable "ami_id" {
  type = string
}
variable "bucket" {
  type = string
}
variable "bucket_arn" {
  type = string
}
variable "release_id" {
  type = string
}
variable "log_group" {
  type = string
}
variable "artifact_key_arn" {
  type = string
}

locals {
  hosts = { "kafka" = { "ip" = "10.42.0.10", "instance" = "t3.medium", "secret" = "platform" }, "clickhouse" = { "ip" = "10.42.0.11", "instance" = "r6i.large", "secret" = "clickhouse" } }
}

resource "aws_ebs_volume" "data" {
  for_each          = local.hosts
  availability_zone = var.az
  size              = 100
  type              = "gp3"
  encrypted         = true
  tags              = { "Name" = "${var.name}-${each.key}-data" }
  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_iam_role" "host" {
  for_each           = local.hosts
  name               = "${var.name}-${each.key}-host"
  assume_role_policy = "{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": {\"Service\": \"ec2.amazonaws.com\"}, \"Action\": \"sts:AssumeRole\"}]}"
}
resource "aws_iam_instance_profile" "host" {
  for_each = local.hosts
  name     = "${var.name}-${each.key}"
  role     = aws_iam_role.host[each.key].name
}
resource "aws_iam_role_policy" "host" {
  for_each = local.hosts
  name     = "runtime-only"
  role     = aws_iam_role.host[each.key].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["ssm:UpdateInstanceInformation", "ssmmessages:CreateControlChannel", "ssmmessages:CreateDataChannel", "ssmmessages:OpenControlChannel", "ssmmessages:OpenDataChannel"], Resource = "*" },
    { Effect = "Allow", Action = ["ecr:GetAuthorizationToken"], Resource = "*" },
    { Effect = "Allow", Action = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"], Resource = [for repo in(each.key == "kafka" ? ["kafka", "postgres"] : ["clickhouse"]) : "arn:aws:ecr:${var.region}:${var.account_id}:repository/${var.name}/${repo}"] },
    { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = var.secrets[each.value.secret] },
    { Effect = "Allow", Action = ["kms:Decrypt"], Resource = var.artifact_key_arn },
    { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${var.bucket_arn}/${var.release_id}/bootstrap/*", "${var.bucket_arn}/${var.release_id}/migrations/${each.key == "kafka" ? "postgres" : "clickhouse"}/*"] },
    { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "arn:aws:logs:${var.region}:${var.account_id}:log-group:${var.log_group}:*" }
  ] })
}
resource "aws_instance" "host" {
  dynamic "credit_specification" {
    for_each = each.key == "kafka" ? [1] : []
    content { cpu_credits = "standard" }
  }
  for_each                    = local.hosts
  ami                         = var.ami_id
  instance_type               = each.value.instance
  subnet_id                   = var.subnet_id
  private_ip                  = each.value.ip
  associate_public_ip_address = false
  vpc_security_group_ids      = [var.groups[each.key], var.groups["operator"]]
  iam_instance_profile        = aws_iam_instance_profile.host[each.key].name
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  root_block_device {
    encrypted             = true
    volume_size           = 20
    volume_type           = "gp3"
    delete_on_termination = true
  }
  user_data                   = templatefile("${path.module}/user-data.sh.tftpl", { region = var.region, bucket = var.bucket, release_id = var.release_id, role = each.key, volume = aws_ebs_volume.data[each.key].id, secret = var.secrets[each.value.secret], images = jsonencode({ for k, v in var.images : k => v if(each.key == "kafka" ? contains(["kafka", "postgres"], k) : k == "clickhouse") }), name = var.name, log_group = var.log_group })
  user_data_replace_on_change = true
  tags                        = { "Name" = "${var.name}-${each.key}" }
  depends_on                  = [aws_iam_role_policy.host]
}
resource "aws_volume_attachment" "data" {
  for_each                       = local.hosts
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.data[each.key].id
  instance_id                    = aws_instance.host[each.key].id
  force_detach                   = false
  stop_instance_before_detaching = true
}

output "instances" {
  value = { for k, v in aws_instance.host : k => v.id }
}

