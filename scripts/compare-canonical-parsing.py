#!/usr/bin/env python3
"""Compare exact partition metrics with separate and single-pass JSON decoding.

Local Docker only. No facts or schemas change. Requires an explicit refresh
pause; restores only the schedules that were active before this diagnostic.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project', default='streamforge')
    p.add_argument('--partition', type=int, default=0)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--pause-refreshes', action='store_true', required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', a.project):
        p.error('invalid local project')
    if not 0 <= a.partition < 16 or not 1 <= a.rounds <= 5:
        p.error('partition must be 0..15; rounds 1..5')
    if a.out.exists():
        p.error('output already exists; preserve earlier evidence')
    cmd = ['docker', 'compose', '-p', a.project, '--env-file', '.env', '-f', 'deploy/compose.yaml',
           'exec', '-T', 'clickhouse', 'sh', '-c',
           'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
           '--database streamforge --multiquery "$@"', 'parse-probe']

    def sql(statement, query_id=None):
        r = subprocess.run(cmd + (['--query_id', query_id] if query_id else []),
                           input=statement, text=True, capture_output=True, timeout=90)
        if r.returncode:
            raise RuntimeError(r.stderr[:1800])
        return r.stdout

    def view_select(path):
        return path.read_text().split('CREATE OR REPLACE VIEW events_canonical AS\n', 1)[1].split(';', 1)[0]

    old = view_select(Path('migrations/clickhouse/006_identity_values_cutover.sql'))
    new = view_select(Path('migrations/clickhouse/008_parse_canonical_payload_once.sql'))
    metric = Path('migrations/clickhouse/007_partitioned_metrics.sql').read_text().split(
        ' TO identity_metrics_00 AS\n', 1)[1].split(';', 1)[0]
    metric = metric.replace('cityHash64(event_id)%16=0', f'cityHash64(event_id)%16={a.partition}')
    queries = {k: metric.replace('FROM events_canonical', 'FROM ('+v+')')
               for k, v in [('separate', old), ('tuple', new)]}
    metadata = [json.loads(line) for line in sql(
        "SELECT view,status FROM system.view_refreshes WHERE database='streamforge' FORMAT JSONEachRow").splitlines()]
    active = [r['view'] for r in metadata if r['status'] in ('Running', 'Scheduled')]
    if any(not re.fullmatch(r'(identity_metrics_[0-9]{2}|revenue_1m|traffic_1m)_refresh', v) for v in active):
        raise RuntimeError('unexpected refresh name; refusing to alter schedules')
    result = {'success': False, 'recorded_at': datetime.now(timezone.utc).isoformat(), 'project': a.project,
              'partition': a.partition, 'scope': 'single retained partition; shared host; alternating order; not SLO acceptance',
              'queries': queries, 'original_refreshes': metadata, 'attempts': []}
    expected = None
    errors = []
    try:
        for v in active:
            sql('SYSTEM STOP VIEW '+v)
        deadline = time.monotonic()+30
        while sql("SELECT count() FROM system.view_refreshes WHERE database='streamforge' AND status='Running'").strip() != '0':
            if time.monotonic() >= deadline:
                raise RuntimeError('refreshes did not settle')
            time.sleep(.5)
        for i in range(a.rounds):
            for label in (['separate', 'tuple'] if i % 2 == 0 else ['tuple', 'separate']):
                item = {'round': i, 'candidate': label, 'query_id': 'parse_'+uuid.uuid4().hex}
                result['attempts'].append(item)
                began = time.monotonic()
                try:
                    rows = [json.loads(line) for line in sql(queries[label]+' FORMAT JSONEachRow', item['query_id']).splitlines()]
                    rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
                    if expected is None:
                        expected = rows
                    item.update(success=True, exact_metrics_equal=rows == expected, metric_rows=len(rows))
                    if rows != expected:
                        raise RuntimeError('exact metric rows differ')
                except Exception as error:
                    item.update(success=False, error=str(error)[:1800])
                    raise
                finally:
                    item['wall_seconds'] = round(time.monotonic()-began, 4)
                    print(json.dumps(item), flush=True)
        sql('SYSTEM FLUSH LOGS')
        ids = ','.join("'"+r['query_id']+"'" for r in result['attempts'])
        logs = sql("SELECT query_id,query_duration_ms,memory_usage,read_rows,read_bytes FROM system.query_log "
                   "PREWHERE event_time>now()-INTERVAL 1 HOUR AND type='QueryFinish' "
                   'WHERE query_id IN ('+ids+') SETTINGS max_threads=1,max_block_size=128,'
                   'max_read_buffer_size=8192,max_memory_usage=134217728 FORMAT JSONEachRow')
        by_id = {r['query_id']: r for r in map(json.loads, logs.splitlines())}
        for item in result['attempts']:
            item['native'] = by_id.get(item['query_id'])
        result['success'] = True
    finally:
        for v in active:
            try:
                sql('SYSTEM START VIEW '+v)
            except Exception as error:
                errors.append({'view': v, 'error': str(error)[:500]})
        result['restore_errors'] = errors
        result['migration_sha256'] = {str(f): hashlib.sha256(f.read_bytes()).hexdigest()
                                     for f in sorted(Path('migrations/clickhouse').glob('*.sql'))}
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(result, indent=2)+'\n')
        if errors:
            raise RuntimeError('one or more refresh schedules failed to restart; see result')


if __name__ == '__main__':
    main()
