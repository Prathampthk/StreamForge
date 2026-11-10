#!/usr/bin/env python3
"""Compare runtime JSON decoding with stored derived fields on one owned copy.

Local only. Temporarily pauses active refreshes, preserves source identities,
then drops only its unique test table and restores those schedules. Results
repeat the same partition at each concurrency: not an end-to-end SLO test.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
import uuid
from reconcile import ClickHouse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--partition', type=int, default=0)
    parser.add_argument('--concurrency', default='1,4,16')
    args = parser.parse_args()
    levels = [int(x) for x in args.concurrency.split(',')]
    if not 0 <= args.partition < 16 or not levels or any(x not in (1, 2, 4, 8, 16) for x in levels):
        parser.error('partition 0..15 and concurrency values from 1,2,4,8,16 required')
    if args.out.exists():
        parser.error('preserve earlier results; choose a new output')
    client = ClickHouse()
    if urlparse(client.url).hostname not in ('localhost', '127.0.0.1', '::1'):
        parser.error('only loopback ClickHouse is permitted')
    table = 'streamforge.decoded_probe_' + uuid.uuid4().hex
    command = ['./scripts/compose.sh', 'exec', '-T', 'clickhouse', 'sh', '-c',
               'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
               '--database streamforge --multiquery']

    def sql(statement):
        r = subprocess.run(command, input=statement, text=True, capture_output=True, timeout=90)
        if r.returncode:
            raise RuntimeError(r.stderr[:1500])
        return r.stdout

    view = Path('migrations/clickhouse/008_parse_canonical_payload_once.sql').read_text().split(
        'CREATE OR REPLACE VIEW events_canonical AS\n', 1)[1].split(';', 1)[0]
    data_type = re.search(r"JSONExtract\(payload_min,'([^']+)'\)", view).group(1)
    runtime = view.replace('decoded', 'runtime_decoded').replace('event_identity_values', table)
    stored = view.replace('event_identity_values', table).replace(
        "JSONExtract(payload_min,'"+data_type+"') AS decoded", 'decoded AS stored_decoded').replace('decoded.', 'stored_decoded.')
    metric = Path('migrations/clickhouse/007_partitioned_metrics.sql').read_text().split(
        ' TO identity_metrics_00 AS\n', 1)[1].split(';', 1)[0]
    metric = metric.replace('cityHash64(event_id)%16=0', f'cityHash64(event_id)%16={args.partition}')
    metric = metric.replace('max_block_size=128', 'max_block_size=4096').replace(
        'max_read_buffer_size=8192', 'max_read_buffer_size=65536').replace(
        'preferred_block_size_bytes=16384', 'preferred_block_size_bytes=131072')
    queries = {label: metric.replace('FROM events_canonical', 'FROM ('+source+')')
               for label, source in [('runtime', runtime), ('stored', stored)]}
    active = sql("SELECT view FROM system.view_refreshes WHERE database='streamforge' AND status IN ('Running','Scheduled')").splitlines()
    if any(not re.fullmatch(r'(identity_metrics_[0-9]{2}|revenue_1m|traffic_1m)_refresh', v) for v in active):
        raise RuntimeError('unexpected view; refusing schedule changes')
    result = {'recorded_at': datetime.now(timezone.utc).isoformat(), 'success': False,
              'scope': 'same copied partition, shared host, warm cache; not full-history concurrent shards or SLO acceptance',
              'partition': args.partition, 'owned_table': table, 'concurrency': levels, 'queries': queries, 'groups': []}
    created = False
    expected = None
    try:
        for v in active:
            sql('SYSTEM STOP VIEW '+v)
        for _ in range(60):
            if sql("SELECT count() FROM system.view_refreshes WHERE database='streamforge' AND status='Running'").strip() == '0':
                break
            time.sleep(.5)
        else:
            raise RuntimeError('refreshes did not settle')
        sql('CREATE TABLE '+table+' AS event_identity_values')
        created = True
        sql('SYSTEM STOP MERGES '+table)
        sql('ALTER TABLE '+table+' ADD COLUMN decoded '+data_type+
            " MATERIALIZED JSONExtract(payload_min,'"+data_type+"')")
        sql('INSERT INTO '+table+' (event_id,payload_min,payload_max) '
            'SELECT event_id,payload_min,payload_max FROM event_identity_values FINAL '
            f'WHERE cityHash64(event_id)%16={args.partition} SETTINGS max_threads=1,max_final_threads=1,'
            'max_memory_usage=268435456,max_execution_time=60,max_block_size=4096,'
            'min_insert_block_size_rows=65536,min_insert_block_size_bytes=0,max_insert_block_size=65536')
        result['copied_rows'] = int(sql('SELECT count() FROM '+table).strip())
        for i, concurrency in enumerate(levels):
            for label in (['runtime', 'stored'] if i % 2 == 0 else ['stored', 'runtime']):
                barrier = threading.Barrier(concurrency)

                def query(_):
                    qid = 'decoded_'+uuid.uuid4().hex
                    request = Request(client.url+'/?'+urlencode(dict(database='streamforge', readonly=2,
                        wait_end_of_query=1, query_id=qid)), data=(queries[label]+' FORMAT JSONEachRow').encode(),
                        headers={'Authorization': client.auth})
                    barrier.wait(timeout=10)
                    began = time.monotonic()
                    item = {'query_id': qid}
                    rows = None
                    try:
                        with urlopen(request, timeout=25) as response:
                            body = response.read((2 << 20)+1)
                        if len(body) > 2 << 20:
                            raise RuntimeError('response exceeds bound')
                        rows = [json.loads(line) for line in body.decode().splitlines()]
                        rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
                        item.update(success=True, metric_rows=len(rows))
                    except HTTPError as error:
                        item.update(success=False, error=error.read(1200).decode(errors='replace'))
                    except Exception as error:
                        item.update(success=False, error=type(error).__name__)
                    item['wall_seconds'] = round(time.monotonic()-began, 4)
                    return item, rows

                began = time.monotonic()
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    responses = list(executor.map(query, range(concurrency)))
                group = {'candidate': label, 'concurrency': concurrency,
                         'wall_seconds': round(time.monotonic()-began, 4), 'queries': []}
                for item, rows in responses:
                    if rows is not None and expected is None:
                        expected = rows
                    item['exact_metrics_equal'] = rows is not None and rows == expected
                    group['queries'].append(item)
                result['groups'].append(group)
                print(json.dumps(group), flush=True)
        result['success'] = all(q['success'] and q['exact_metrics_equal']
                                for g in result['groups'] for q in g['queries'])
    finally:
        cleanup_errors = []
        if created:
            try:
                sql('DROP TABLE '+table+' SYNC')
            except Exception as error:
                cleanup_errors.append(str(error)[:500])
        for v in active:
            try:
                sql('SYSTEM START VIEW '+v)
            except Exception as error:
                cleanup_errors.append({'view': v, 'error': str(error)[:500]})
        result['cleanup_errors'] = cleanup_errors
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2)+'\n')
        if cleanup_errors:
            raise RuntimeError('fixture cleanup/restoration failed; see result')
    if not result['success']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
