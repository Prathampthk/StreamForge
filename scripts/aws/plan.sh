#!/bin/sh
# Authenticated PLAN ONLY, after profile/account setup; never apply or destroy.
set -eu
cd "$(dirname "$0")/../.."
: "${AWS_PROFILE:?select an authenticated AWS profile}"
: "${AWS_ACCOUNT_ID:?select the reviewed 12-digit account}"
: "${AWS_REGION:?select the reviewed region}"
command -v aws >/dev/null
actual=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
test "$actual" = "$AWS_ACCOUNT_ID"
test -f infra/terraform/envs/dev/backend.tf
if ! git diff --quiet || ! git diff --cached --quiet; then echo 'Commit reviewed source before planning.' >&2; exit 1; fi
mkdir -p .aws-artifacts
# Profile cache is read-only: perform aws sso login on the host first.
docker run --rm -e AWS_PROFILE -e AWS_REGION -v "$HOME/.aws:/root/.aws:ro" -v "$PWD:/src" -w /src/infra/terraform/envs/dev hashicorp/terraform:1.17.1@sha256:a5d5a5a892c98f60fcbfa81eac2a40fc19e7b3fa7719bccae10ff6111d22524a init -reconfigure -input=false -backend-config=/src/.aws-artifacts/backend.hcl
exec docker run --rm -e AWS_PROFILE -e AWS_REGION -v "$HOME/.aws:/root/.aws:ro" -v "$PWD:/src" -w /src/infra/terraform/envs/dev hashicorp/terraform:1.17.1@sha256:a5d5a5a892c98f60fcbfa81eac2a40fc19e7b3fa7719bccae10ff6111d22524a plan -input=false -var="account_id=$AWS_ACCOUNT_ID" -var="region=$AWS_REGION" -var-file=/src/.aws-artifacts/deployment.tfvars.json -out=/src/.aws-artifacts/review.tfplan
