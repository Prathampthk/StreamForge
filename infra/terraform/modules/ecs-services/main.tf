variable "name" {
  type = string
}
variable "region" {
  type = string
}
variable "account_id" {
  type = string
}
variable "vpc_id" {
  type = string
}
variable "subnets" {
  type = list(string)
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
variable "tenant" {
  type = string
}
variable "counts" {
  type = map(number)
}
variable "target_groups" {
  type    = map(string)
  default = {}
}

locals {
  apps = { "ingest-api" = { "secret" = "ingest", "cpu" = 256, "memory" = 512, "port" = 8080, "entry" = "/ingest-api", "env" = { "HTTP_ADDR" = "0.0.0.0:8080", "INGEST_TENANT" = var.tenant, "KAFKA_BROKERS" = "kafka.${var.name}.internal:9094", "KAFKA_TLS" = "true" }, "keys" = ["INGEST_API_KEY", "KAFKA_CA_PEM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"] }, "consumer" = { "secret" = "consumer", "cpu" = 512, "memory" = 1024, "port" = 8081, "entry" = "/consumer", "env" = { "CONSUMER_HTTP_ADDR" = "0.0.0.0:8081", "KAFKA_BROKERS" = "kafka.${var.name}.internal:9094", "KAFKA_TLS" = "true", "CLICKHOUSE_URL" = "https://clickhouse.${var.name}.internal:8443", "CLICKHOUSE_USER" = "sf_consumer", "CONSUMER_BATCH_RECORDS" = "1000", "CONSUMER_BATCH_BYTES" = "8388608" }, "keys" = ["KAFKA_CA_PEM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "CLICKHOUSE_CA_PEM", "CLICKHOUSE_PASSWORD", "POSTGRES_DSN", "POSTGRES_CA_PEM"] }, "query-api" = { "secret" = "query", "cpu" = 256, "memory" = 512, "port" = 8082, "entry" = "/query-api", "env" = { "QUERY_HTTP_ADDR" = "0.0.0.0:8082", "QUERY_TENANT" = var.tenant, "CLICKHOUSE_URL" = "https://clickhouse.${var.name}.internal:8443", "CLICKHOUSE_USER" = "sf_query" }, "keys" = ["QUERY_API_KEY", "CLICKHOUSE_CA_PEM", "CLICKHOUSE_PASSWORD"] }, "replayctl" = { "secret" = "replay", "cpu" = 256, "memory" = 512, "port" = 0, "entry" = "/replayctl", "env" = { "KAFKA_BROKERS" = "kafka.${var.name}.internal:9094", "KAFKA_TLS" = "true" }, "keys" = ["KAFKA_CA_PEM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "POSTGRES_DSN", "POSTGRES_CA_PEM"] } }
}

resource "aws_ecs_cluster" "this" {
  name = var.name
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}
resource "aws_service_discovery_private_dns_namespace" "apps" {
  name = "apps.${var.name}.internal"
  vpc  = var.vpc_id
}
resource "aws_service_discovery_service" "app" {
  for_each = { for k, v in local.apps : k => v if k != "replayctl" }
  name     = each.key
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.apps.id
    dns_records {
      type = "A"
      ttl  = 10
    }
    routing_policy = "MULTIVALUE"
  }
}
resource "aws_iam_role" "execution" {
  for_each           = local.apps
  name               = "${var.name}-${each.key}-execution"
  assume_role_policy = "{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": {\"Service\": \"ecs-tasks.amazonaws.com\"}, \"Action\": \"sts:AssumeRole\"}]}"
}
resource "aws_iam_role" "app" {
  for_each           = local.apps
  name               = "${var.name}-${each.key}-app"
  assume_role_policy = "{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Principal\": {\"Service\": \"ecs-tasks.amazonaws.com\"}, \"Action\": \"sts:AssumeRole\"}]}"
}
resource "aws_cloudwatch_log_group" "app" {
  for_each          = local.apps
  name              = "/${var.name}/${each.key}"
  retention_in_days = 7
}
resource "aws_iam_role_policy" "execution" {
  for_each = local.apps
  role     = aws_iam_role.execution[each.key].id
  policy   = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["ecr:GetAuthorizationToken"], Resource = "*" }, { Effect = "Allow", Action = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"], Resource = "arn:aws:ecr:${var.region}:${var.account_id}:repository/${var.name}/${each.key == "replayctl" ? "tools" : each.key}" }, { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.app[each.key].arn}:*" }, { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = var.secrets[each.value.secret] }] })
}
resource "aws_ecs_task_definition" "app" {
  for_each                 = local.apps
  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(each.value.cpu)
  memory                   = tostring(each.value.memory)
  execution_role_arn       = aws_iam_role.execution[each.key].arn
  task_role_arn            = aws_iam_role.app[each.key].arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
  container_definitions = jsonencode([merge({ name = each.key, image = var.images[each.key == "replayctl" ? "tools" : each.key], essential = true, user = "65532:65532", readonlyRootFilesystem = true, entryPoint = [each.value.entry], linuxParameters = { capabilities = { drop = ["ALL"] } }, stopTimeout = 120, environment = [for k, v in merge(each.value.env, { TRACE_SAMPLE_RATIO = "0.1" }) : { name = k, value = v }], secrets = [for k in each.value.keys : { name = k, valueFrom = "${var.secrets[each.value.secret]}:${k}::" }], logConfiguration = { logDriver = "awslogs", options = { awslogs-group = aws_cloudwatch_log_group.app[each.key].name, awslogs-region = var.region, awslogs-stream-prefix = "app" } } }, each.key == "replayctl" ? { portMappings = [], healthCheck = null } : { portMappings = [{ containerPort = each.value.port, protocol = "tcp" }], healthCheck = { command = ["CMD", each.value.entry, "healthcheck"], interval = 30, timeout = 5, retries = 3, startPeriod = 60 } })])
}
resource "aws_ecs_service" "app" {
  for_each               = { for k, v in local.apps : k => v if k != "replayctl" }
  name                   = each.key
  cluster                = aws_ecs_cluster.this.id
  task_definition        = aws_ecs_task_definition.app[each.key].arn
  desired_count          = var.counts[each.key]
  launch_type            = "FARGATE"
  platform_version       = "1.4.0"
  enable_execute_command = false
  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
  network_configuration {
    subnets          = var.subnets
    security_groups  = [var.groups[each.value.secret]]
    assign_public_ip = false
  }
  service_registries {
    registry_arn = aws_service_discovery_service.app[each.key].arn
  }
  depends_on = [aws_iam_role_policy.execution]
  dynamic "load_balancer" {
    for_each = contains(keys(var.target_groups), each.key) ? [var.target_groups[each.key]] : []
    content {
      target_group_arn = load_balancer.value
      container_name   = each.key
      container_port   = each.value.port
    }
  }
}

output "cluster" {
  value = aws_ecs_cluster.this.name
}
output "task_definitions" {
  value = { for k, v in aws_ecs_task_definition.app : k => v.arn }
}
