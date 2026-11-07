#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm --network "${GO_DOCKER_NETWORK:-bridge}" -e KAFKA_BROKERS -v "$PWD:/src" -v streamforge_go_mod:/go/pkg/mod -v streamforge_go_build:/root/.cache/go-build -w /src golang:1.26.0 "$@"
