#!/usr/bin/env python3
"""Rehearse native database recovery in owned, network-isolated containers.

Never connects to the retained stack. Private archives are removed on exit.
This small fixture does not measure production RPO/RTO or Kafka recovery.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path)
    args = parser.parse_args()
    images = json.loads(Path('deploy/vendor-images.json').read_text())
    prefix = 'streamforge-restore-' + uuid.uuid4().hex[:10]
    owned = set()
    started = time.monotonic()
    result = {'fixture_only': True, 'recorded_at': datetime.now(timezone.utc).isoformat(),
              'images': images, 'network': 'none'}

    def run(argv, **kwargs):
        try:
            return subprocess.run(argv, check=True, capture_output=True, timeout=120, **kwargs)
        except subprocess.CalledProcessError as error:
            print(error.stderr.decode(errors="replace")[-3000:], file=sys.stderr)
            raise

    def remove(name):
        run(['docker', 'rm', '-f', '-v', name])
        owned.remove(name)

    with tempfile.TemporaryDirectory(prefix='sf-private-backup-') as directory:
        root = Path(directory)
        root.chmod(0o700)
        config = root / 'backups.xml'
        config.write_text('<clickhouse><backups><allowed_path>/backups/</allowed_path></backups>'
                          '<max_server_memory_usage>1073741824</max_server_memory_usage>'
                          '<mark_cache_size>33554432</mark_cache_size></clickhouse>')

        def start(kind, suffix, password, user):
            name = prefix + '-' + suffix
            env = os.environ.copy()
            if kind == 'clickhouse':
                env.update(CLICKHOUSE_USER=user, CLICKHOUSE_PASSWORD=password,
                           CLICKHOUSE_DB='streamforge')
                options = ['--memory', '1536m', '--tmpfs', '/backups:rw,size=128m,uid=101,gid=101',
                           '-v', str(config) + ':/etc/clickhouse-server/config.d/backups.xml:ro']
                keys = ['CLICKHOUSE_USER', 'CLICKHOUSE_PASSWORD', 'CLICKHOUSE_DB']
            else:
                env.update(POSTGRES_USER=user, POSTGRES_PASSWORD=password, POSTGRES_DB='postgres')
                options = ['--memory', '512m']
                keys = ['POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB']
            command = ['docker', 'run', '-d', '--name', name, '--network', 'none', *options]
            for key in keys:
                command += ['-e', key]
            run(command + [images[kind]], env=env)
            owned.add(name)
            for _ in range(90):
                probe = (['docker', 'exec', name, 'sh', '-c',
                          'case "$(cat /proc/1/comm)" in clickhouse*) ;; *) exit 1;; esac; clickhouse-client '
                          '--user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"']
                         if kind == 'clickhouse' else
                         ['docker', 'exec', name, 'sh', '-c',
                          'test "$(cat /proc/1/comm)" = postgres && pg_isready -U "$POSTGRES_USER"'])
                if subprocess.run(probe, capture_output=True, timeout=5).returncode == 0:
                    return name
                time.sleep(1)
            raise RuntimeError(kind + ' did not become ready')

        def ch(name, statement):
            return run(['docker', 'exec', '-i', name, 'sh', '-c',
                        'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
                        '--database streamforge --multiquery'], input=statement.encode()).stdout.decode().strip()

        def pg(name, user, statement, database='streamforge'):
            return run(['docker', 'exec', '-i', name, 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                        '-U', user, '-d', database], input=statement.encode()).stdout.decode().strip()

        try:
            source = start('clickhouse', 'ch-source', secrets.token_urlsafe(24), 'fixture_owner')
            for migration in sorted(Path('migrations/clickhouse').glob('*.sql')):
                ch(source, migration.read_text())
            at = datetime.now(timezone.utc)
            stamp = at.strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
            rows = []
            for i in range(64):
                event = dict(schema_version=1, event_id=str(uuid.uuid5(uuid.NAMESPACE_DNS, 'backup-'+str(i))),
                             tenant_id='backup_fixture', user_id='user', event_type='purchase',
                             occurred_at=at.isoformat(), order_id='order_'+str(i), amount_cents=i+1, currency='USD')
                rows.append(event | dict(occurred_at=stamp, received_at=stamp, kafka_topic='fixture',
                                        kafka_partition=0, kafka_offset=i, sink_batch_id='backup',
                                        payload_json=json.dumps(event, sort_keys=True, separators=(',', ':'))))
            changed = rows[0] | {'amount_cents': 999}
            changed['payload_json'] = json.dumps(json.loads(rows[0]['payload_json']) | {'amount_cents': 999},
                                               sort_keys=True, separators=(',', ':'))
            ch(source, 'INSERT INTO events_raw FORMAT JSONEachRow\n' +
               '\n'.join(json.dumps(row) for row in rows + rows[:2] + [changed]))
            for _ in range(60):
                totals = ch(source, 'SELECT sum(purchase_count),sum(revenue_cents) FROM revenue_1m_canonical')
                if totals == '63\t2079':
                    break
                time.sleep(1)
            else:
                raise RuntimeError('source projection totals did not converge: '+totals)
            views = ch(source, "SELECT view FROM system.view_refreshes WHERE database='streamforge'").splitlines()
            for view in views:
                ch(source, 'SYSTEM STOP VIEW ' + view)
            for _ in range(30):
                if ch(source, "SELECT count() FROM system.view_refreshes WHERE database='streamforge' AND status='Running'") == '0':
                    break
                time.sleep(1)
            else:
                raise RuntimeError('fixture refreshes did not stop')
            queries = {
                'raw': 'SELECT * FROM events_raw ORDER BY event_id,payload_json FORMAT JSONEachRow',
                'identity_strings': 'SELECT * FROM event_identity_values FINAL ORDER BY event_id FORMAT JSONEachRow',
                'canonical': 'SELECT * FROM events_canonical ORDER BY event_id FORMAT JSONEachRow',
                'conflicts': 'SELECT * FROM event_identity_conflicts ORDER BY event_id FORMAT JSONEachRow',
                'revenue': 'SELECT * FROM revenue_1m_canonical ORDER BY tenant_id,minute,currency FORMAT JSONEachRow',
                'traffic': 'SELECT * FROM traffic_1m_canonical ORDER BY tenant_id,minute FORMAT JSONEachRow',
                'ledger': 'SELECT * FROM schema_migrations ORDER BY version FORMAT JSONEachRow'}
            expected = {key: ch(source, sql) for key, sql in queries.items()}
            assert len(expected['raw'].splitlines()) == 67
            assert len(expected['canonical'].splitlines()) == 63
            assert len(expected['conflicts'].splitlines()) == 1
            ch(source, "BACKUP DATABASE streamforge TO File('/backups/fixture.zip')")
            archive = root / 'clickhouse.zip'
            archive.write_bytes(run(['docker', 'exec', source, 'cat', '/backups/fixture.zip']).stdout)
            archive.chmod(0o600)
            remove(source)
            target = start('clickhouse', 'ch-target', secrets.token_urlsafe(24), 'new_owner')
            # The server's empty startup database must be absent for a genuine clean restore.
            ch(target, 'DROP DATABASE streamforge')
            run(['docker', 'exec', '-i', '-u', '101:101', target, 'sh', '-c',
                 'cat > /backups/fixture.zip'], input=archive.read_bytes())
            run(['docker', 'exec', '-i', target, 'sh', '-c',
                 'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'],
                input=b"RESTORE DATABASE streamforge FROM File('/backups/fixture.zip')")
            for key, sql in queries.items():
                if ch(target, sql) != expected[key]:
                    raise RuntimeError('ClickHouse restore mismatch: '+key)
            result['clickhouse'] = {'status': 'pass', 'raw_rows': 67, 'canonical_ids': 63,
                                    'conflicts': 1, 'revenue_cents': 2079,
                                    'exact_comparisons': list(queries), 'archive_bytes': archive.stat().st_size,
                                    'source_volume_removed_before_restore': True, 'new_owner_credentials': True}
            remove(target)

            source = start('postgres', 'pg-source', secrets.token_urlsafe(24), 'streamforge')
            pg(source, 'streamforge', 'CREATE DATABASE streamforge', 'postgres')
            for migration in sorted(Path('migrations/postgres').glob('*.up.sql')):
                pg(source, 'streamforge', migration.read_text())
            pg(source, 'streamforge', "INSERT INTO tenants(tenant_id,name) VALUES('backup_fixture','Backup');"
               "CREATE ROLE fixture_reader LOGIN; GRANT CONNECT ON DATABASE streamforge TO fixture_reader;"
               "GRANT USAGE ON SCHEMA public TO fixture_reader; GRANT SELECT ON tenants TO fixture_reader;")
            dump = run(['docker', 'exec', source, 'pg_dump', '-U', 'streamforge', '-Fc', 'streamforge']).stdout
            globals_dump = run(['docker', 'exec', source, 'pg_dumpall', '-U', 'streamforge',
                                '--globals-only', '--no-role-passwords']).stdout
            remove(source)
            target = start('postgres', 'pg-target', secrets.token_urlsafe(24), 'restore_admin')
            pg(target, 'restore_admin', globals_dump.decode(), 'postgres')
            run(['docker', 'exec', '-i', target, 'pg_restore', '--create', '--exit-on-error',
                 '-U', 'restore_admin', '-d', 'postgres'], input=dump)
            assert pg(target, 'restore_admin', "SELECT tenant_id||':'||name FROM tenants") == 'backup_fixture:Backup'
            assert pg(target, 'restore_admin', "SELECT has_table_privilege('fixture_reader','tenants','SELECT'),"
                      "has_table_privilege('fixture_reader','api_keys','SELECT')") == 't|f'
            assert pg(target, 'restore_admin', "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='tenants'::regclass") == 'streamforge'
            result['postgres'] = {'status': 'pass', 'custom_dump_bytes': len(dump),
                                  'globals_without_passwords': True, 'owner_and_reader_acl_restored': True,
                                  'source_volume_removed_before_restore': True}
            remove(target)
            result.update(success=True, elapsed_seconds=round(time.monotonic()-started, 3))
            if args.out:
                args.out.parent.mkdir(parents=True, exist_ok=True)
                args.out.write_text(json.dumps(result, indent=2)+'\n')
            print(json.dumps(result, indent=2))
        finally:
            for name in list(owned):
                subprocess.run(['docker', 'rm', '-f', '-v', name], capture_output=True, timeout=30)


if __name__ == '__main__':
    main()
