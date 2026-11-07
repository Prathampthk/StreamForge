#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
for file in migrations/postgres/*.up.sql; do
  ./scripts/compose.sh exec -T postgres psql -v ON_ERROR_STOP=1 -U streamforge -d streamforge < "$file"
done
for file in migrations/clickhouse/*.sql; do
  ./scripts/compose.sh exec -T clickhouse sh -c 'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery' < "$file"
done
for topic in streamforge.events.v1 streamforge.events.dlq.v1; do
 ./scripts/compose.sh exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic "$topic" --partitions 12 --replication-factor 1 --config retention.ms=604800000 --config max.message.bytes=1048576 --config min.insync.replicas=1 --config unclean.leader.election.enable=false
done

./scripts/compose.sh exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:19092 --alter --entity-type topics --entity-name streamforge.events.dlq.v1 --add-config max.message.bytes=2097152
