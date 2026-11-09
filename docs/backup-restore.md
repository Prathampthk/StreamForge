# Local recovery rehearsal

Run from the repository root with Docker available:

```sh
python3 scripts/check-backup-restore.py --out docs/evidence/backup-restore.json
```

This creates unique owned containers without networking or published ports. It uses the repository's pinned vendor images. It never connects to the retained stack. Cleanup removes only the containers and anonymous volumes it created. Small private archives live in a temporary directory with restricted permissions and are removed on exit.

ClickHouse uses native database BACKUP/RESTORE, after fixture refreshes stop. The test preserves duplicate raw rows, exact full min/max identity strings, quarantined conflicts, canonical payloads, revenue, traffic and migration ledger. It removes the source container and volume, restores into a fresh server with different owner credentials, then compares every fixture row and exact metric result. PostgreSQL uses a custom-format pg_dump plus globals without role passwords, restores into a new cluster, and checks metadata ownership and allowed/denied reader privileges. Credentials must be managed independently.

This is a small recovery test, not a backup of the real retained dataset. It does not measure production restore time, establish a recovery point objective, prove off-host durability or provide a transactional backup across Kafka, PostgreSQL and ClickHouse. The source is quiescent during its backup. A real recovery needs an access-controlled encrypted destination, retention policy, independently stored keys, exact manifest/version checks and a restore into separate infrastructure before any cutover. Never restore over the retained database for a rehearsal.

After restoring an older ClickHouse snapshot, the Kafka consumer's committed offsets may be ahead of restored data. Replay from a reviewed earlier offset or new group while Kafka still retains those records; canonical identity semantics handle repeated identical events. Kafka retention must cover the chosen restore point, or records need a separate durable archive. The base topic's seven-day retention is not a database backup. PostgreSQL metadata/audit and ClickHouse facts have no shared transaction or coordinated recovery point.

Native database backups also do not constitute a backup of Compose files, service users/grants, TLS material, secret values, Kafka logs/metadata or runtime pause/detach state. Preserve configuration and restore those operational boundaries explicitly. A frozen legacy identity table requires reverse backfill before rollback after new traffic.
