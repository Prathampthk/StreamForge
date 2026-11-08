variable "name" { type = string }
resource "aws_ecr_repository" "image" {
  for_each             = toset(["ingest-api", "consumer", "query-api", "tools", "kafka", "clickhouse", "postgres"])
  name                 = "${var.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}
output "urls" { value = { for k, v in aws_ecr_repository.image : k => v.repository_url } }
output "scanning_enabled" { value = alltrue([for r in aws_ecr_repository.image : r.image_scanning_configuration[0].scan_on_push]) }
