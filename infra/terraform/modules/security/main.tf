variable "name" {
  type = string
}
variable "vpc_id" {
  type = string
}
variable "endpoint_sg" {
  type = string
}
variable "s3_prefix_list" {
  type = string
}
variable "client_cidrs" {
  type    = list(string)
  default = []
}

resource "aws_security_group" "ingest" {
  name_prefix = "${var.name}-ingest-"
  vpc_id      = var.vpc_id
  description = "ingest project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "query" {
  name_prefix = "${var.name}-query-"
  vpc_id      = var.vpc_id
  description = "query project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "consumer" {
  name_prefix = "${var.name}-consumer-"
  vpc_id      = var.vpc_id
  description = "consumer project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "operator" {
  name_prefix = "${var.name}-operator-"
  vpc_id      = var.vpc_id
  description = "operator project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "kafka" {
  name_prefix = "${var.name}-kafka-"
  vpc_id      = var.vpc_id
  description = "kafka project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "clickhouse" {
  name_prefix = "${var.name}-clickhouse-"
  vpc_id      = var.vpc_id
  description = "clickhouse project boundary"
  ingress     = []
  egress      = []
}
resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  vpc_id      = var.vpc_id
  description = "alb project boundary"
  ingress     = []
  egress      = []
}
resource "aws_vpc_security_group_egress_rule" "ingest_api" {
  security_group_id            = aws_security_group.ingest.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "ingest_s3" {
  security_group_id = aws_security_group.ingest.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "query_api" {
  security_group_id            = aws_security_group.query.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "query_s3" {
  security_group_id = aws_security_group.query.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "consumer_api" {
  security_group_id            = aws_security_group.consumer.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "consumer_s3" {
  security_group_id = aws_security_group.consumer.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "operator_api" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "operator_s3" {
  security_group_id = aws_security_group.operator.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "kafka_api" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "kafka_s3" {
  security_group_id = aws_security_group.kafka.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "clickhouse_api" {
  security_group_id            = aws_security_group.clickhouse.id
  referenced_security_group_id = var.endpoint_sg
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_security_group_egress_rule" "clickhouse_s3" {
  security_group_id = aws_security_group.clickhouse.id
  prefix_list_id    = var.s3_prefix_list
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "ingest_kafka_9094" {
  security_group_id            = aws_security_group.ingest.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_egress_rule" "consumer_kafka_9094" {
  security_group_id            = aws_security_group.consumer.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_egress_rule" "operator_kafka_9094" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_egress_rule" "consumer_kafka_5432" {
  security_group_id            = aws_security_group.consumer.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
resource "aws_vpc_security_group_egress_rule" "operator_kafka_5432" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = aws_security_group.kafka.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
resource "aws_vpc_security_group_egress_rule" "consumer_clickhouse_8443" {
  security_group_id            = aws_security_group.consumer.id
  referenced_security_group_id = aws_security_group.clickhouse.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_egress_rule" "query_clickhouse_8443" {
  security_group_id            = aws_security_group.query.id
  referenced_security_group_id = aws_security_group.clickhouse.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_egress_rule" "operator_clickhouse_8443" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = aws_security_group.clickhouse.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_egress_rule" "alb_ingest_8080" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.ingest.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_egress_rule" "alb_query_8082" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.query.id
  ip_protocol                  = "tcp"
  from_port                    = 8082
  to_port                      = 8082
}
resource "aws_vpc_security_group_egress_rule" "operator_ingest_8080" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = aws_security_group.ingest.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_egress_rule" "operator_query_8082" {
  security_group_id            = aws_security_group.operator.id
  referenced_security_group_id = aws_security_group.query.id
  ip_protocol                  = "tcp"
  from_port                    = 8082
  to_port                      = 8082
}
resource "aws_vpc_security_group_ingress_rule" "ingest_kafka_9094" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.ingest.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_ingress_rule" "consumer_kafka_9094" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.consumer.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_ingress_rule" "operator_kafka_9094" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.operator.id
  ip_protocol                  = "tcp"
  from_port                    = 9094
  to_port                      = 9094
}
resource "aws_vpc_security_group_ingress_rule" "consumer_kafka_5432" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.consumer.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
resource "aws_vpc_security_group_ingress_rule" "operator_kafka_5432" {
  security_group_id            = aws_security_group.kafka.id
  referenced_security_group_id = aws_security_group.operator.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}
resource "aws_vpc_security_group_ingress_rule" "consumer_clickhouse_8443" {
  security_group_id            = aws_security_group.clickhouse.id
  referenced_security_group_id = aws_security_group.consumer.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_ingress_rule" "query_clickhouse_8443" {
  security_group_id            = aws_security_group.clickhouse.id
  referenced_security_group_id = aws_security_group.query.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_ingress_rule" "operator_clickhouse_8443" {
  security_group_id            = aws_security_group.clickhouse.id
  referenced_security_group_id = aws_security_group.operator.id
  ip_protocol                  = "tcp"
  from_port                    = 8443
  to_port                      = 8443
}
resource "aws_vpc_security_group_ingress_rule" "alb_ingest_8080" {
  security_group_id            = aws_security_group.ingest.id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_ingress_rule" "alb_query_8082" {
  security_group_id            = aws_security_group.query.id
  referenced_security_group_id = aws_security_group.alb.id
  ip_protocol                  = "tcp"
  from_port                    = 8082
  to_port                      = 8082
}
resource "aws_vpc_security_group_ingress_rule" "operator_ingest_8080" {
  security_group_id            = aws_security_group.ingest.id
  referenced_security_group_id = aws_security_group.operator.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}
resource "aws_vpc_security_group_ingress_rule" "operator_query_8082" {
  security_group_id            = aws_security_group.query.id
  referenced_security_group_id = aws_security_group.operator.id
  ip_protocol                  = "tcp"
  from_port                    = 8082
  to_port                      = 8082
}
resource "aws_vpc_security_group_ingress_rule" "https" {
  for_each          = toset(var.client_cidrs)
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.key
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_secretsmanager_secret" "service" {
  for_each                = toset(["platform", "clickhouse", "ingest", "consumer", "query", "replay"])
  name                    = "${var.name}/${each.key}"
  recovery_window_in_days = 30
  lifecycle {
    prevent_destroy = true
  }
}

output "groups" {
  value = { "ingest" = aws_security_group.ingest.id, "query" = aws_security_group.query.id, "consumer" = aws_security_group.consumer.id, "operator" = aws_security_group.operator.id, "kafka" = aws_security_group.kafka.id, "clickhouse" = aws_security_group.clickhouse.id, "alb" = aws_security_group.alb.id }
}
output "secrets" {
  value = { for k, v in aws_secretsmanager_secret.service : k => v.arn }
}
