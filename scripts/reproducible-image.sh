#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
task_epoch=$(git show -s --format=%ct HEAD)
for pass in one two; do
 docker build --no-cache --provenance=false --output type=docker,rewrite-timestamp=true --build-arg "SOURCE_DATE_EPOCH=$task_epoch" -f deploy/docker/ingest.Dockerfile -t "streamforge-repro:$pass" .
done
task_one=$(docker image inspect --format '{{.Id}}' streamforge-repro:one)
task_two=$(docker image inspect --format '{{.Id}}' streamforge-repro:two)
test "$task_one" = "$task_two"
printf 'PASS independent runtime image IDs match: %s\n' "$task_one"
