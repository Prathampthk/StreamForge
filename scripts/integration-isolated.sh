#!/bin/sh
# Fresh owned infrastructure; existing StreamForge data/ports are untouched.
set -eu
cd "$(dirname "$0")/.."
project=${STREAMFORGE_TEST_PROJECT:-streamforge-phase6-check}
docker volume create streamforge_go_mod >/dev/null
docker volume create streamforge_go_build >/dev/null
compose() { docker compose -p "$project" --env-file .env -f deploy/compose.yaml -f deploy/compose.consumer.yaml -f deploy/compose.ci.yaml "$@"; }
cleanup() { compose down --remove-orphans; }
trap cleanup EXIT HUP INT TERM
compose up -d --wait --wait-timeout 240 kafka postgres clickhouse
for file in migrations/postgres/*.up.sql; do compose exec -T postgres psql -v ON_ERROR_STOP=1 -U streamforge -d streamforge < "$file"; done
for file in migrations/clickhouse/*.sql; do compose exec -T clickhouse sh -c 'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery' < "$file"; done
for topic in streamforge.events.v1 streamforge.events.dlq.v1; do compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic "$topic" --partitions 12 --replication-factor 1 --config max.message.bytes=2097152; done
compose run --rm --no-deps integration-tests
