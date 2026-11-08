mock_provider "aws" {
  mock_resource "aws_iam_role" { defaults = { arn = "arn:aws:iam::123456789012:role/mock" } }
  mock_resource "aws_iam_openid_connect_provider" { defaults = { arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com" } }
  mock_resource "aws_cloudwatch_log_group" { defaults = { arn = "arn:aws:logs:us-east-2:123456789012:log-group:mock" } }
  mock_resource "aws_ecs_cluster" { defaults = { id = "arn:aws:ecs:us-east-2:123456789012:cluster/mock" } }
  mock_resource "aws_ecs_task_definition" { defaults = { arn = "arn:aws:ecs:us-east-2:123456789012:task-definition/mock:1" } }
  mock_resource "aws_service_discovery_service" { defaults = { arn = "arn:aws:servicediscovery:us-east-2:123456789012:service/srv-1234567890" } }
  mock_resource "aws_lb" { defaults = { arn = "arn:aws:elasticloadbalancing:us-east-2:123456789012:loadbalancer/app/mock/1234567890123456", zone_id = "Z1234567890", dns_name = "mock.us-east-2.elb.amazonaws.com" } }
  mock_resource "aws_lb_target_group" { defaults = { arn = "arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/mock/1234567890123456" } }
  mock_resource "aws_lb_listener" { defaults = { arn = "arn:aws:elasticloadbalancing:us-east-2:123456789012:listener/app/mock/1234567890123456/1234567890123456" } }
}
variables {
  account_id = "123456789012"
  release_id = "1111111111111111111111111111111111111111"
}
run "private_bootstrap" {
  command = plan
  assert {
    condition     = length(module.network.public_subnets) == 0 && length(aws_lb.public) == 0 && length(module.apps) == 0 && length(module.stateful) == 0 && module.registry.scanning_enabled
    error_message = "Default bootstrap must not create public exposure or start unconfigured services."
  }
}
run "reject_missing_images" {
  command = plan
  variables { enable_stateful = true }
  expect_failures = [terraform_data.gate]
}
run "reject_open_public_access" {
  command = plan
  variables { client_cidrs = ["0.0.0.0/0"] }
  expect_failures = [var.client_cidrs]
}
run "enabled_private_services" {
  command = plan
  variables {
    enable_stateful = true
    enable_apps     = true
    ami_id          = "ami-0123456789abcdef0"
    images = {
      kafka      = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/kafka@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      clickhouse = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/clickhouse@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      postgres   = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/postgres@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      ingest-api = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/ingest-api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      consumer   = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/consumer@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      query-api  = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/query-api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      tools      = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/tools@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
  assert {
    condition     = length(module.stateful[0].instances) == 2 && length(module.apps) == 1
    error_message = "Stateful nodes must remain private and require IMDSv2."
  }
}

run "public_https_only" {
  command = plan
  variables {
    enable_stateful = true
    public_access   = true
    certificate_arn = "arn:aws:acm:us-east-2:123456789012:certificate/11111111-1111-1111-1111-111111111111"
    public_hostname = "demo.example.com"
    public_zone_id  = "Z0123456789ABCDEF"
    client_cidrs    = ["198.51.100.10/32"]
    enable_apps     = true
    ami_id          = "ami-0123456789abcdef0"
    images = {
      kafka      = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/kafka@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      clickhouse = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/clickhouse@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      postgres   = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/postgres@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      ingest-api = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/ingest-api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      consumer   = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/consumer@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      query-api  = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/query-api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      tools      = "123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/tools@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }
  assert {
    condition     = length(aws_lb.public) == 1 && aws_lb_listener.https[0].port == 443 && aws_lb_listener.https[0].protocol == "HTTPS"
    error_message = "Stateful nodes must remain private and require IMDSv2."
  }
}
