variable "name" {
  type = string
}
variable "region" {
  type = string
}
variable "azs" {
  type = list(string)
}
variable "public_access" {
  type    = bool
  default = false
}

resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { "Name" = var.name }
}
resource "aws_default_security_group" "closed" {
  vpc_id  = aws_vpc.this.id
  ingress = []
  egress  = []
}
resource "aws_subnet" "private" {
  count                   = 2
  vpc_id                  = aws_vpc.this.id
  availability_zone       = var.azs[count.index]
  cidr_block              = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index)
  map_public_ip_on_launch = false
}
resource "aws_subnet" "public" {
  count                   = var.public_access ? 2 : 0
  vpc_id                  = aws_vpc.this.id
  availability_zone       = var.azs[count.index]
  cidr_block              = cidrsubnet(aws_vpc.this.cidr_block, 8, 100 + count.index)
  map_public_ip_on_launch = false
}
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
}
resource "aws_route_table" "public" {
  count  = var.public_access ? 1 : 0
  vpc_id = aws_vpc.this.id
}
resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}
resource "aws_route_table_association" "public" {
  count          = var.public_access ? 2 : 0
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}
resource "aws_internet_gateway" "this" {
  count  = var.public_access ? 1 : 0
  vpc_id = aws_vpc.this.id
}
resource "aws_route" "internet" {
  count                  = var.public_access ? 1 : 0
  route_table_id         = aws_route_table.public[0].id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this[0].id
}
resource "aws_security_group" "endpoints" {
  name_prefix = "${var.name}-endpoints-"
  vpc_id      = aws_vpc.this.id
  description = "Private AWS API HTTPS from project private subnets only"
  ingress     = [{ "from_port" = 443, "to_port" = 443, "protocol" = "tcp", "cidr_blocks" = ["10.42.0.0/23"], "description" = "Project HTTPS endpoints", "ipv6_cidr_blocks" = [], "prefix_list_ids" = [], "security_groups" = [], "self" = false }]
  egress      = []
}
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}
resource "aws_vpc_endpoint" "apis" {
  for_each            = toset(["ecr.api", "ecr.dkr", "logs", "secretsmanager", "ssm", "ssmmessages"])
  vpc_id              = aws_vpc.this.id
  service_name        = "com.amazonaws.${var.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [aws_subnet.private[0].id]
  security_group_ids  = [aws_security_group.endpoints.id]
}

output "vpc_id" {
  value = aws_vpc.this.id
}
output "private_subnets" {
  value = aws_subnet.private[*].id
}
output "public_subnets" {
  value = aws_subnet.public[*].id
}
output "endpoint_sg" {
  value = aws_security_group.endpoints.id
}
output "s3_prefix_list" {
  value = aws_vpc_endpoint.s3.prefix_list_id
}
