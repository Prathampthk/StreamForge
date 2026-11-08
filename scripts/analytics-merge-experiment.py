#!/usr/bin/env python3
"""Compare exact canonical merges in an owned, disposable ClickHouse container."""
import argparse
from datetime import datetime, timezone
import platform
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import uuid

IMAGE = 'clickhouse/clickhouse-server:25.8.4.13@sha256:c48c1ed0a54e6e9fde03750860aa86de1e6f40b6fe33c6df540738867b4f6809'
BASELINE = "SELECT event_id,minMerge(payload_min) AS payload,maxMerge(payload_max) AS last_payload FROM identities GROUP BY event_id HAVING payload=last_payload"
CANDIDATE = "SELECT event_id,finalizeAggregation(payload_min) AS payload,finalizeAggregation(payload_max) AS last_payload FROM identities FINAL WHERE payload=last_payload"
SETTINGS = 'max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864,max_execution_time=60,max_block_size=1024,max_read_buffer_size=65536,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sizes', default='10000,100000,500000,1000000')
    parser.add_argument('--refresh', action='store_true', help='also run the real migration and both scheduled refreshes after seeding')
    parser.add_argument('--memory', choices=['1g','2g','4g'], default='2g')
    parser.add_argument('--output', default='load/results/raw/analytics-merge/result.json')
    args = parser.parse_args()
    sizes = [int(v) for v in args.sizes.split(',')]
    if not sizes or sizes != sorted(set(sizes)) or sizes[0] < 10 or sizes[-1] > 20000000:
        parser.error('sizes must increase from >=10 to <=20000000')
    name = 'streamforge-merge-experiment-' + uuid.uuid4().hex[:12]
    password = secrets.token_urlsafe(32)
    started = False
    results = {'recorded_at': datetime.now(timezone.utc).isoformat(), 'platform': platform.platform(), 'docker_resources': subprocess.check_output(['docker','info','--format','cpus={{.NCPU}} memory={{.MemTotal}}'],text=True).strip(), 'image': IMAGE, 'settings': SETTINGS, 'container_memory': args.memory, 'candidate_runs_second': True, 'sizes': [], 'scope': 'isolated synthetic query experiment; not an end-to-end benchmark'}

    def sql(statement, query_id=None, check=True, database=None, multiquery=False):
        cmd = ['docker', 'exec', '-i', name, 'clickhouse-client', '--config-file', '/tmp/experiment.xml']
        if database:
            cmd += ['--database', database]
        if multiquery:
            cmd += ['--multiquery']
        if query_id:
            cmd += ['--query_id', query_id]
        result = subprocess.run(cmd, input=statement, text=True, capture_output=True, timeout=150)
        if check and result.returncode:
            raise RuntimeError(result.stderr[:2000])
        return result

    try:
        subprocess.run(['docker', 'run', '--rm', '-d', '--name', name, '--network', 'none', '--memory', args.memory,
                        '-v', '/var/lib/clickhouse', '-v', str(Path('deploy/clickhouse/resources.xml').resolve()) + ':/etc/clickhouse-server/config.d/resources.xml:ro',
                        '-e', 'CLICKHOUSE_USER=experiment', '-e', 'CLICKHOUSE_PASSWORD', '-e', 'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1', IMAGE],
                       env=os.environ | {'CLICKHOUSE_PASSWORD': password}, check=True, stdout=subprocess.DEVNULL)
        started = True
        subprocess.run(['docker', 'exec', '-i', name, 'sh', '-c', 'umask 077; cat > /tmp/experiment.xml'],
                       input='<config><user>experiment</user><password>' + password + '</password></config>', text=True, check=True)
        for _ in range(60):
            process = subprocess.run(['docker', 'exec', name, 'cat', '/proc/1/comm'], capture_output=True, text=True)
            if process.stdout.strip().startswith('clickhouse') and sql('SELECT 1', check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError('server not ready')
        sql('CREATE TABLE identities(event_id UUID,payload_min AggregateFunction(min,String),payload_max AggregateFunction(max,String)) ENGINE=AggregatingMergeTree PARTITION BY cityHash64(event_id)%16 ORDER BY event_id')
        sql('SYSTEM STOP MERGES identities')
        previous = 0
        for size in sizes:
            # Synthetic JSON strings exercise state size; this is not an event-contract or Kafka load test.
            payload = "toJSONString(map('schema_version','1','event_id',toString(id),'tenant_id','experiment','user_id',concat('user_',toString(number%10000)),'event_type',if(number%10=0,'purchase','api_request'),'occurred_at','2026-11-07T12:00:00Z','amount_cents',toString(number%10000),'currency','USD','status_code',if(number%7=0,'503','200'),'attributes',repeat('abcdef',24)))"
            for start in range(previous, size, 50000):
                source = f"SELECT reinterpretAsUUID(toUInt128(number+1)) AS id,{payload} AS payload FROM numbers({start},{min(50000,size-start)})"
                sql(f"INSERT INTO identities SELECT id,initializeAggregation('minState',payload),initializeAggregation('maxState',payload) FROM ({source}) SETTINGS max_threads=1,max_block_size=4096")
            # Different insert blocks produce unmerged duplicates and conflicts (ID 1).
            sql('INSERT INTO identities SELECT * FROM identities WHERE event_id IN (reinterpretAsUUID(toUInt128(1)),reinterpretAsUUID(toUInt128(2))) LIMIT 2')
            sql("INSERT INTO identities SELECT reinterpretAsUUID(toUInt128(1)),minState('changed tenant/minute/currency'),maxState('changed tenant/minute/currency')")
            row = {'identities': size, 'physical_rows': int(sql('SELECT count() FROM identities').stdout), 'queries': {}}
            for label, source_query in [('group_by', BASELINE), ('final', CANDIDATE)]:
                identifier = 'merge_' + uuid.uuid4().hex
                query = f"SELECT count(),sum(cityHash64(payload)),sum(toInt128(JSONExtractInt(payload,'amount_cents'))) FROM ({source_query}) SETTINGS {SETTINGS} FORMAT JSONEachRow"
                before = time.monotonic()
                result = sql(query, identifier, check=False)
                item = {'success': result.returncode == 0, 'wall_seconds': round(time.monotonic()-before, 3)}
                if result.returncode:
                    item['error'] = result.stderr[:1200]
                else:
                    item['result'] = json.loads(result.stdout)
                sql('SYSTEM FLUSH LOGS')
                log = sql(f"SELECT query_duration_ms,memory_usage,read_rows,read_bytes,type FROM system.query_log WHERE query_id='{identifier}' AND type IN ('QueryFinish','ExceptionWhileProcessing') ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSONEachRow").stdout.strip()
                item['query_log'] = json.loads(log) if log else None
                row['queries'][label] = item
            if row['queries']['final']['success']:
                assert row['queries']['final']['result']['count()'] == size - 1, row
                expected_amount = (size // 10000)*49995000 + (size % 10000)*(size % 10000-1)//2
                assert row['queries']['final']['result']["sum(toInt128(JSONExtractInt(payload, 'amount_cents')))"] == expected_amount, row
            if all(v['success'] for v in row['queries'].values()):
                assert row['queries']['group_by']['result'] == row['queries']['final']['result'], row
            results['sizes'].append(row)
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(results, indent=2)+'\n')
            print(json.dumps(row), flush=True)
            if not row['queries']['final']['success']:
                raise RuntimeError('candidate query failed; see recorded result')
            previous = size
        if args.refresh:
            sql('CREATE DATABASE streamforge')
            sql('RENAME TABLE identities TO streamforge.event_identity_states')
            for file in sorted(Path('migrations/clickhouse').glob('*.sql')):
                sql(file.read_text(), database='streamforge', multiquery=True)
                if file.name.startswith('002_'):
                    sql('SYSTEM STOP VIEW streamforge.revenue_1m_refresh')
                    sql('SYSTEM STOP VIEW streamforge.traffic_1m_refresh')
            for view in ['revenue_1m_refresh', 'traffic_1m_refresh']:
                sql('SYSTEM START VIEW streamforge.' + view)
                sql('SYSTEM REFRESH VIEW streamforge.' + view)
            started_at = time.monotonic()
            expected_errors = (size-1)//7 - (size-1)//70
            purchases_including_zero = (size+9)//10
            expected = [size-1, size-purchases_including_zero, expected_errors, purchases_including_zero-1, (size//10000)*4995000 + sum(range(0,size%10000,10))]
            statement = "SELECT (SELECT sum(events_per_minute) FROM streamforge.traffic_1m_canonical),(SELECT sum(rpm) FROM streamforge.traffic_1m_canonical),(SELECT sum(api_errors) FROM streamforge.traffic_1m_canonical),(SELECT sum(purchase_count) FROM streamforge.revenue_1m_canonical),(SELECT sum(revenue_cents) FROM streamforge.revenue_1m_canonical) FORMAT TabSeparated"
            while time.monotonic()-started_at < 90:
                actual = [int(v) for v in sql(statement).stdout.split()]
                if actual == expected:
                    break
                time.sleep(1)
            else:
                raise AssertionError({'scheduled_refresh_actual': actual, 'expected': expected})
            health = sql("SELECT view,status,last_success_time,exception,read_rows FROM system.view_refreshes WHERE database='streamforge' FORMAT JSONEachRow").stdout
            results['scheduled_refresh'] = {'success': True, 'actual': actual, 'expected': expected, 'wait_seconds': round(time.monotonic()-started_at,3), 'health': [json.loads(line) for line in health.splitlines()]}
            Path(args.output).write_text(json.dumps(results, indent=2)+'\n')
            print(json.dumps(results['scheduled_refresh']), flush=True)
    finally:
        if started:
            # Remove only this invocation's anonymous volume/container.
            subprocess.run(['docker', 'stop', '--time', '10', name], check=True, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
