#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# No AWS credentials are mounted. Authenticated plans use a reviewed host session.
task_tf_dir=${TF_DIR:-infra/terraform/envs/dev}
case "$task_tf_dir" in infra/terraform|infra/terraform/*) ;; *) echo "Terraform directory must be inside infra/terraform" >&2; exit 2;; esac
exec docker run --rm -v "$PWD:/src" -w "/src/$task_tf_dir" hashicorp/terraform:1.17.1@sha256:a5d5a5a892c98f60fcbfa81eac2a40fc19e7b3fa7719bccae10ff6111d22524a "$@"
