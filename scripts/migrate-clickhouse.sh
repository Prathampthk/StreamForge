#!/bin/sh
# Apply only unapplied ClickHouse migrations. Never replay retired read paths.
set -eu
cd "$(dirname "$0")/.."
project=${STREAMFORGE_COMPOSE_PROJECT:-streamforge}
ch() {
 docker compose -p "$project" --env-file .env -f deploy/compose.yaml exec -T clickhouse sh -c 'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery'
}
for file in migrations/clickhouse/*.sql; do
 version=$(basename "$file" | cut -d_ -f1)
 case "$version" in *[!0-9]*|'') echo 'Invalid migration filename' >&2; exit 1;; esac
 ledger=$(printf '%s\n' 'EXISTS TABLE schema_migrations' | ch)
 if [ "$ledger" = 1 ]; then
  applied=$(printf 'SELECT count() FROM schema_migrations WHERE version=%s\n' "$version" | ch)
  if [ "$applied" != 0 ]; then continue; fi
 fi
 ch < "$file"
 ledger=$(printf '%s\n' 'EXISTS TABLE schema_migrations' | ch)
 if [ "$ledger" = 1 ]; then
  printf 'INSERT INTO schema_migrations SELECT %s,now64(3) WHERE (SELECT count() FROM schema_migrations WHERE version=%s)=0\n' "$version" "$version" | ch
 fi
done
