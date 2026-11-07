#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm --network "${GO_DOCKER_NETWORK:-bridge}" -e KAFKA_BROKERS -v "$PWD:/src" -v streamforge_go_mod:/go/pkg/mod -v streamforge_go_build:/root/.cache/go-build -w /src golang:1.26.10@sha256:cfa27959837144bf4649802ba38e59daffd0dd2a7994aa83daaf113285b0f551 "$@"
