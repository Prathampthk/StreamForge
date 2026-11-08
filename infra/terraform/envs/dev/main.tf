terraform {
  required_version = "= 1.17.1"
  required_providers {
    aws = { "source" = "hashicorp/aws", "version" = "= 6.72.0" }
  }
}

provider "aws" {
  region              = var.region
  allowed_account_ids = [var.account_id]
  default_tags {
    tags = { "Project" = var.name, "Environment" = "dev", "ManagedBy" = "Terraform" }
  }
}

variable "name" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,19}$", var.name))
    error_message = "Use a short lowercase project name."
  }
  default = "streamforge-dev"
}
variable "account_id" {
  type = string
  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "Provide the explicitly selected AWS account ID."
  }
}
variable "region" {
  type    = string
  default = "us-east-2"
}
variable "azs" {
  type = list(string)
  validation {
    condition     = length(var.azs) == 2 && length(toset(var.azs)) == 2
    error_message = "Provide two distinct region availability zones."
  }
  default = ["us-east-2a", "us-east-2b"]
}
variable "ami_id" {
  type    = string
  default = ""
}
variable "release_id" {
  type = string
  validation {
    condition     = can(regex("^[a-f0-9]{40}$", var.release_id))
    error_message = "Use the exact reviewed Git commit SHA."
  }
}
variable "images" {
  type    = map(string)
  default = {}
}
variable "enable_stateful" {
  type    = bool
  default = false
}
variable "enable_apps" {
  type    = bool
  default = false
}
variable "public_access" {
  type    = bool
  default = false
}
variable "certificate_arn" {
  type    = string
  default = ""
}
variable "client_cidrs" {
  type = list(string)
  validation {
    condition     = alltrue([for c in var.client_cidrs : can(cidrnetmask(c)) && c != "0.0.0.0/0"])
    error_message = "Use explicit IPv4 client networks; unrestricted access is forbidden."
  }
  default = []
}
variable "public_hostname" {
  type    = string
  default = ""
}
variable "public_zone_id" {
  type    = string
  default = ""
}
variable "tenant" {
  type    = string
  default = "merchant_demo"
}
variable "service_counts" {
  type = map(number)
  validation {
    condition     = toset(keys(var.service_counts)) == toset(["ingest-api", "consumer", "query-api"]) && alltrue([for n in values(var.service_counts) : n >= 0 && n <= 3 && floor(n) == n])
    error_message = "Specify all three service counts, each 0..3."
  }
  default = { "ingest-api" = 1, "consumer" = 1, "query-api" = 1 }
}
variable "github_repository" {
  type    = string
  default = "Prathampthk/StreamForge"
}
variable "github_oidc_provider_arn" {
  type    = string
  default = ""
}

locals {
  bundle_files = setunion(fileset("${path.module}/../../../..", "migrations/**/*.sql"), toset(["scripts/aws/stateful.py"]))
}

module "network" {
  source        = "../../modules/network"
  name          = var.name
  region        = var.region
  azs           = var.azs
  public_access = var.public_access
}
module "security" {
  source         = "../../modules/security"
  name           = var.name
  vpc_id         = module.network.vpc_id
  endpoint_sg    = module.network.endpoint_sg
  s3_prefix_list = module.network.s3_prefix_list
  client_cidrs   = var.client_cidrs
}
module "registry" {
  source = "../../modules/registry"
  name   = var.name
}
module "stateful" {
  count            = var.enable_stateful ? 1 : 0
  source           = "../../modules/stateful"
  name             = var.name
  region           = var.region
  account_id       = var.account_id
  az               = var.azs[0]
  subnet_id        = module.network.private_subnets[0]
  groups           = module.security.groups
  secrets          = module.security.secrets
  images           = var.images
  ami_id           = var.ami_id
  bucket           = aws_s3_bucket.artifacts.id
  bucket_arn       = aws_s3_bucket.artifacts.arn
  release_id       = var.release_id
  log_group        = aws_cloudwatch_log_group.stateful.name
  depends_on       = [terraform_data.gate, aws_s3_object.bundle]
  artifact_key_arn = aws_kms_key.artifacts.arn
}
module "apps" {
  count         = var.enable_apps ? 1 : 0
  source        = "../../modules/ecs-services"
  name          = var.name
  region        = var.region
  account_id    = var.account_id
  vpc_id        = module.network.vpc_id
  subnets       = [module.network.private_subnets[0]]
  groups        = module.security.groups
  secrets       = module.security.secrets
  images        = var.images
  tenant        = var.tenant
  counts        = var.service_counts
  target_groups = { for k, v in aws_lb_target_group.app : k => v.arn }
  depends_on    = [terraform_data.gate]
}

resource "terraform_data" "gate" {
  input = var.release_id
  lifecycle {
    precondition {
      condition     = !var.enable_stateful || can(regex("^ami-[a-f0-9]+$", var.ami_id))
      error_message = "Stateful hosts require a reviewed x86_64 AMI with Docker, AWS CLI v2 and SSM Agent."
    }
    precondition {
      condition     = !var.enable_apps || var.enable_stateful
      error_message = "Enable stateful services before applications."
    }
    precondition {
      condition     = !var.public_access || (var.enable_apps && length(var.client_cidrs) > 0 && var.certificate_arn != "" && var.public_hostname != "" && var.public_zone_id != "")
      error_message = "Public access requires enabled apps, ACM, DNS zone/hostname and explicit client CIDRs."
    }
    precondition {
      condition     = !(var.enable_stateful || var.enable_apps) || alltrue([for k in ["kafka", "postgres", "clickhouse", "ingest-api", "query-api", "consumer", "tools"] : can(regex("^[0-9]{12}[.]dkr[.]ecr[.][a-z0-9-]+[.]amazonaws[.]com/[a-z0-9/_-]+@sha256:[a-f0-9]{64}$", lookup(var.images, k, "")))])
      error_message = "Use all seven immutable private ECR image digests."
    }
  }
}
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.name}-${var.account_id}-${var.region}-artifacts"
  force_destroy = false
  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_s3_bucket" "alb_logs" {
  count         = var.public_access ? 1 : 0
  bucket        = "${var.name}-${var.account_id}-${var.region}-alb-logs"
  force_destroy = false
  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_public_access_block" "alb_logs" {
  count                   = var.public_access ? 1 : 0
  bucket                  = aws_s3_bucket.alb_logs[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}
resource "aws_s3_bucket_versioning" "alb_logs" {
  count  = var.public_access ? 1 : 0
  bucket = aws_s3_bucket.alb_logs[0].id
  versioning_configuration {
    status = "Enabled"
  }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.artifacts.arn
    }
  }
}
resource "aws_s3_bucket_policy" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  policy = jsonencode({ Version = "2012-10-17", Statement = concat([{ Sid = "DenyInsecureTransport", Effect = "Deny", Principal = "*", Action = "s3:*", Resource = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"], Condition = { Bool = { "aws:SecureTransport" = "false" } } }], var.public_access ? [{ Effect = "Allow", Principal = { Service = "logdelivery.elasticloadbalancing.amazonaws.com" }, Action = "s3:PutObject", Resource = "${aws_s3_bucket.artifacts.arn}/alb/AWSLogs/${var.account_id}/*" }] : []) })
}
resource "aws_s3_bucket_policy" "alb_logs" {
  count  = var.public_access ? 1 : 0
  bucket = aws_s3_bucket.alb_logs[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Deny", Principal = "*", Action = "s3:*", Resource = [aws_s3_bucket.alb_logs[0].arn, "${aws_s3_bucket.alb_logs[0].arn}/*"], Condition = { Bool = { "aws:SecureTransport" = "false" } } }, { Effect = "Allow", Principal = { Service = "logdelivery.elasticloadbalancing.amazonaws.com" }, Action = "s3:PutObject", Resource = "${aws_s3_bucket.alb_logs[0].arn}/alb/AWSLogs/${var.account_id}/*" }] })
}
resource "aws_s3_object" "bundle" {
  for_each               = local.bundle_files
  bucket                 = aws_s3_bucket.artifacts.id
  key                    = "${var.release_id}/${each.key == "scripts/aws/stateful.py" ? "bootstrap/stateful.py" : each.key}"
  source                 = "${path.module}/../../../../${each.key}"
  source_hash            = filesha256("${path.module}/../../../../${each.key}")
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.artifacts.arn
}
resource "aws_s3_object" "manifest" {
  bucket                 = aws_s3_bucket.artifacts.id
  key                    = "${var.release_id}/bootstrap/manifest.json"
  content                = jsonencode(sort(tolist(local.bundle_files)))
  server_side_encryption = "aws:kms"
  kms_key_id             = aws_kms_key.artifacts.arn
}
resource "aws_cloudwatch_log_group" "stateful" {
  name              = "/${var.name}/stateful"
  retention_in_days = 7
}
resource "aws_cloudwatch_log_group" "flow" {
  name              = "/${var.name}/vpc-flow"
  retention_in_days = 7
}
resource "aws_iam_role" "flow" {
  name               = "${var.name}-flow-logs"
  assume_role_policy = "{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": {\"Service\": \"vpc-flow-logs.amazonaws.com\"}, \"Action\": \"sts:AssumeRole\"}]}"
}
resource "aws_iam_role" "publisher" {
  count                = var.enable_publisher_role ? 1 : 0
  name                 = "${var.name}-github-publisher"
  max_session_duration = 3600
  assume_role_policy   = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Principal = { Federated = var.github_oidc_provider_arn != "" ? var.github_oidc_provider_arn : aws_iam_openid_connect_provider.github[0].arn }, Action = "sts:AssumeRoleWithWebIdentity", Condition = { StringEquals = { "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com", "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:environment:aws-dev-publish" } } }] })
}
resource "aws_iam_role_policy" "flow" {
  role   = aws_iam_role.flow.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"], Resource = "${aws_cloudwatch_log_group.flow.arn}:*" }] })
}
resource "aws_iam_role_policy" "publisher" {
  count  = var.enable_publisher_role ? 1 : 0
  role   = aws_iam_role.publisher[0].id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["ecr:GetAuthorizationToken"], Resource = "*" }, { Effect = "Allow", Action = ["ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:DescribeImages", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"], Resource = [for k, v in module.registry.urls : "arn:aws:ecr:${var.region}:${var.account_id}:repository/${var.name}/${k}"] }] })
}
resource "aws_flow_log" "vpc" {
  vpc_id          = module.network.vpc_id
  traffic_type    = "ALL"
  log_destination = aws_cloudwatch_log_group.flow.arn
  iam_role_arn    = aws_iam_role.flow.arn
}
resource "aws_route53_zone" "private" {
  name = "${var.name}.internal"
  vpc {
    vpc_id = module.network.vpc_id
  }
}
resource "aws_route53_record" "private" {
  for_each = { "kafka" = "10.42.0.10", "metadata" = "10.42.0.10", "clickhouse" = "10.42.0.11" }
  zone_id  = aws_route53_zone.private.zone_id
  name     = "${each.key}.${var.name}.internal"
  type     = "A"
  ttl      = 30
  records  = [each.value]
}
resource "aws_route53_record" "public" {
  count   = var.public_access ? 1 : 0
  zone_id = var.public_zone_id
  name    = var.public_hostname
  type    = "A"
  alias {
    name                   = aws_lb.public[0].dns_name
    zone_id                = aws_lb.public[0].zone_id
    evaluate_target_health = true
  }
}
resource "aws_lb" "public" {
  count                      = var.public_access ? 1 : 0
  name                       = var.name
  internal                   = false
  load_balancer_type         = "application"
  subnets                    = module.network.public_subnets
  security_groups            = [module.security.groups["alb"]]
  enable_deletion_protection = true
  drop_invalid_header_fields = true
  access_logs {
    bucket  = aws_s3_bucket.alb_logs[0].id
    prefix  = "alb"
    enabled = true
  }
  depends_on = [aws_s3_bucket_policy.alb_logs]
}
resource "aws_lb_target_group" "app" {
  for_each    = var.public_access ? { "ingest-api" = 8080, "query-api" = 8082 } : {}
  name        = "${var.name}-${each.key}"
  port        = each.value
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = module.network.vpc_id
  health_check {
    path    = "/readyz"
    matcher = "200"
  }
  deregistration_delay = 30
}
resource "aws_lb_listener" "https" {
  count             = var.public_access ? 1 : 0
  load_balancer_arn = aws_lb.public[0].arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn
  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "application/json"
      message_body = "{\"code\":\"not_found\"}"
      status_code  = "404"
    }
  }
}
resource "aws_lb_listener_rule" "app" {
  for_each     = aws_lb_target_group.app
  listener_arn = aws_lb_listener.https[0].arn
  priority     = each.key == "ingest-api" ? 10 : 20
  action {
    type             = "forward"
    target_group_arn = each.value.arn
  }
  condition {
    path_pattern {
      values = each.key == "ingest-api" ? ["/v1/events", "/v1/events/batch"] : ["/v1/metrics/revenue", "/v1/metrics/traffic"]
    }
  }
}
resource "aws_iam_openid_connect_provider" "github" {
  count          = var.enable_publisher_role && var.github_oidc_provider_arn == "" ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_kms_key" "artifacts" {
  description             = "StreamForge artifact encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  lifecycle {
    prevent_destroy = true
  }
}
resource "aws_kms_alias" "artifacts" {
  name          = "alias/${var.name}/artifacts"
  target_key_id = aws_kms_key.artifacts.key_id
}

output "repositories" {
  value = module.registry.urls
}
output "secret_arns" {
  value = module.security.secrets
}
output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.id
}
output "instances" {
  value = var.enable_stateful ? module.stateful[0].instances : {}
}
output "private_subnets" {
  value = module.network.private_subnets
}
output "groups" {
  value = module.security.groups
}
output "cluster" {
  value = var.enable_apps ? module.apps[0].cluster : ""
}
output "public_url" {
  value = var.public_access ? "https://${var.public_hostname}" : ""
}
output "publisher_role_arn" {
  value = var.enable_publisher_role ? aws_iam_role.publisher[0].arn : ""
}


variable "enable_publisher_role" {
  type    = bool
  default = false
}
