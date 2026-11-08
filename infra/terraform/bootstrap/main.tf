terraform {
  required_version = "= 1.17.1"
  required_providers { aws = { source = "hashicorp/aws", version = "= 6.72.0" } }
}
variable "account_id" {
  type = string
  validation {
    condition     = can(regex("^[0-9]{12}$", var.account_id))
    error_message = "Select the reviewed AWS account explicitly."
  }
}
variable "region" {
  type    = string
  default = "us-east-2"
}
variable "name" {
  type    = string
  default = "streamforge-dev"
}
provider "aws" {
  region              = var.region
  allowed_account_ids = [var.account_id]
  default_tags { tags = { Project = var.name, Environment = "dev", ManagedBy = "Terraform" } }
}
resource "aws_kms_key" "state" {
  description             = "StreamForge Terraform state encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket" "state" {
  bucket        = "${var.name}-${var.account_id}-${var.region}-state"
  force_destroy = false
  lifecycle { prevent_destroy = true }
}
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.state.arn
    }
  }
}
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Deny", Principal = "*", Action = "s3:*", Resource = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"], Condition = { Bool = { "aws:SecureTransport" = "false" } } }] })
}
output "backend_config" {
  value = { bucket = aws_s3_bucket.state.id, key = "streamforge/dev.tfstate", region = var.region, encrypt = true, kms_key_id = aws_kms_key.state.arn, use_lockfile = true, allowed_account_ids = [var.account_id] }
}
