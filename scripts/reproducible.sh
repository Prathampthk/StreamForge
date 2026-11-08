#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if ! command -v go >/dev/null; then exec ./scripts/go.sh ./scripts/reproducible.sh; fi
task_build_dir=$(mktemp -d)
trap 'rm -rf "$task_build_dir"' EXIT HUP INT TERM
for service in ingest-api consumer query-api replayctl eventgen; do
 CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o "$task_build_dir/one" "./cmd/$service"
 CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o "$task_build_dir/two" "./cmd/$service"
 cmp "$task_build_dir/one" "$task_build_dir/two"
 printf '%s: identical binaries\n' "$service"
done
