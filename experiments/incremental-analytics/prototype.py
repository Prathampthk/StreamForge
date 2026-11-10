#!/usr/bin/env python3
"""Owned ClickHouse experiment; no retained database, Kafka or AWS connection.

A serialized worker publishes contiguous immutable generations. This is a
storage/algorithm probe, not a production consumer or an exactly-once claim.
"""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
LIMITS = ' SETTINGS max_threads=1,max_memory_usage=67108864,max_execution_time=30,max_block_size=4096,output_format_json_quote_64bit_integers=0'
FIELDS = ('events', 'rpm', 'errors', 'purchases', 'cents')


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, capture_output=True, timeout=kwargs.pop('timeout', 90), **kwargs)


class Database:
    def __init__(self, container, database):
        if not re.fullmatch(r'streamforge-incremental-[a-f0-9]{10}', container):
            raise ValueError('only an owned incremental fixture container is permitted')
        if not re.fullmatch(r'probe_[a-z0-9_]+', database):
            raise ValueError('only a probe database is permitted')
        self.container, self.database = container, database
        self.measurements = []

    def sql(self, query, measured=False):
        qid = 'incremental_' + uuid.uuid4().hex
        started = time.perf_counter()
        command = ['docker', 'exec', '-i', self.container, 'clickhouse-client',
                   '--database', self.database, '--query_id', qid, '--multiquery']
        try:
            response = run(command, input=query.encode())
        except subprocess.CalledProcessError as error:
            message = error.stderr.decode(errors='replace')
            raise RuntimeError(message[:1800] + ('\n[remaining diagnostics omitted]' if len(message) > 1800 else '')) from error
        raw = response.stdout.decode().strip()
        if measured:
            parsed = json.loads(raw)
            self.measurements.append(dict(query_id=qid, wall_seconds=time.perf_counter()-started,
                                          **parsed['statistics']))
            return parsed['data']
        return raw

    def select(self, query):
        return self.sql(query + LIMITS + ' FORMAT JSON', measured=True)

    def insert(self, table, rows):
        if table not in ('states', 'deltas', 'plans', 'commits'):
            raise ValueError('unexpected table')
        if rows:
            self.sql('INSERT INTO ' + table + ' SETTINGS max_memory_usage=67108864,async_insert=0,insert_deduplicate=0 '
                     'FORMAT JSONEachRow\n' + '\n'.join(encode(row) for row in rows))

    def head(self):
        rows = self.select('SELECT generation FROM commits ORDER BY generation DESC LIMIT 1')
        return int(rows[0]['generation']) if rows else 0

    def states(self, ids, head):
        values = ','.join("'" + str(uuid.UUID(key)) + "'" for key in sorted(ids))
        rows = self.select('SELECT event_id,'
                           'argMax(payload_min,generation) AS lo,argMax(payload_max,generation) AS hi '
                           f'FROM states WHERE generation<={head} AND event_id IN ({values}) GROUP BY event_id') if ids else []
        return {row['event_id']: (row['lo'], row['hi']) for row in rows}

    def metrics(self, head=None, tenant=None, start=None, stop=None):
        if head is None:
            head = self.head()
        filters = [f'generation<={head}']
        # Only deterministic fixture values enter SQL; never a serving API.
        for value in (tenant, start, stop):
            if value is not None and not re.fullmatch(r'[a-zA-Z0-9_ :\-]+', value):
                raise ValueError('unsafe fixture filter')
        if tenant is not None:
            filters.append("tenant='" + tenant + "'")
        if start is not None:
            filters.append("minute>='" + start + "'")
        if stop is not None:
            filters.append("minute<'" + stop + "'")
        rows = self.select('SELECT tenant,minute,kind,currency,' +
                           ','.join('sum(' + field + ') AS ' + field for field in FIELDS) +
                           ' FROM deltas FINAL WHERE ' + ' AND '.join(filters) +
                           ' GROUP BY tenant,minute,kind,currency ORDER BY tenant,minute,kind,currency')
        return {tuple(row[key] for key in ('tenant', 'minute', 'kind', 'currency')):
                tuple(int(row[field]) for field in FIELDS) for row in rows
                if any(int(row[field]) for field in FIELDS)}

    def initialize(self):
        self.sql(HERE.joinpath('schema.sql').read_text())


def contribution(payload):
    if payload is None:
        return {}
    event = json.loads(payload)
    minute = event['occurred_at'][:16].replace('T', ' ') + ':00'
    api = event['event_type'] == 'api_request'
    rows = {(event['tenant_id'], minute, 1, ''): (1, int(api), int(api and event.get('status_code', 0) >= 500), 0, 0)}
    if event['event_type'] == 'purchase':
        rows[event['tenant_id'], minute, 2, event['currency']] = (0, 0, 0, 1, event['amount_cents'])
    return rows


def compute(events, previous, generation):
    incoming = defaultdict(list)
    for event in events:
        key = str(uuid.UUID(event['event_id']))
        if key != event['event_id']:
            raise ValueError('fixture input must already be normalized')
        incoming[key].append(encode(event))
    states, delta = [], defaultdict(lambda: [0]*5)
    for key, payloads in sorted(incoming.items()):
        old = previous.get(key)
        low = min(payloads + ([old[0]] if old else []))
        high = max(payloads + ([old[1]] if old else []))
        if old == (low, high):
            continue
        states.append(dict(event_id=key, generation=generation, payload_min=low, payload_max=high))
        before = old[0] if old and old[0] == old[1] else None
        after = low if low == high else None
        for sign, payload in ((-1, before), (1, after)):
            for bucket, values in contribution(payload).items():
                for index, value in enumerate(values):
                    delta[bucket][index] += sign*value
    rows = [dict(zip(('tenant', 'minute', 'kind', 'currency'), key), generation=generation,
                 **dict(zip(FIELDS, values))) for key, values in sorted(delta.items()) if any(values)]
    return dict(states=states, deltas=rows)


def worker(args):
    # Same-host prototype exclusion only; NOT distributed fencing.
    with (args.batch.parent / 'writer.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('single-writer lock busy') from None
        db = Database(args.container, args.database)
        source = args.batch.read_text()
        events = json.loads(source)
        if not events or len(events) > 4096:
            raise ValueError('fixture batch must have 1..4096 normalized events')
        head = db.head()
        if args.generation <= head:
            committed = db.select(f'SELECT DISTINCT input_json FROM commits WHERE generation={args.generation}')
            if len(committed) != 1 or committed[0]['input_json'] != source:
                raise RuntimeError('committed input mismatch')
            print(encode(dict(already_committed=True, generation=args.generation)))
            return
        if args.generation != head + 1:
            raise RuntimeError('generation gap refused')
        previous = db.states({event['event_id'] for event in events}, head)
        lookup = db.measurements[-1]
        plan = compute(events, previous, args.generation)
        text = encode(plan)
        existing = db.select(f'SELECT DISTINCT input_json,plan_json FROM plans WHERE generation={args.generation}')
        if any(row['input_json'] != source or row['plan_json'] != text for row in existing):
            raise RuntimeError('immutable generation mismatch')
        def boundary(stage):
            if args.fault == stage:
                os._exit(86)  # Real child-process death; no Python cleanup.
            if args.fault == 'hold_' + stage:
                args.batch.with_suffix('.ready').write_text('ready')
                deadline = time.monotonic()+20
                while not args.batch.with_suffix('.release').exists():
                    if time.monotonic() > deadline:
                        raise RuntimeError('fixture release timeout')
                    time.sleep(.05)

        boundary('before_plan')
        db.insert('plans', [dict(generation=args.generation, input_json=source, plan_json=text)])
        boundary('after_plan')
        db.insert('states', plan['states'])
        boundary('after_states')
        db.insert('deltas', plan['deltas'])
        boundary('after_deltas')
        # Retry tables are compared using exact logical rows before publication.
        changed_ids = ','.join("'"+row['event_id']+"'" for row in plan['states'])
        actual = db.select(f'SELECT DISTINCT event_id,generation,payload_min,payload_max FROM states WHERE generation={args.generation} AND event_id IN ({changed_ids})') if changed_ids else []
        expected = plan['states']
        if sorted(encode(row) for row in actual) != sorted(encode(row) for row in expected):
            raise RuntimeError('identity generation verification failed')
        # Bound expression construction as well as data reads: thousands of
        # OR predicates can exhaust the query budget even on an empty table.
        actual = []
        for offset in range(0, len(plan['deltas']), 128):
            chunk = plan['deltas'][offset:offset+128]
            bucket_filter = ' OR '.join("(tenant="+literal(row['tenant'])+" AND minute=toDateTime("+literal(row['minute'])+",'UTC') AND kind="+str(row['kind'])+" AND currency="+literal(row['currency'])+")" for row in chunk)
            actual.extend(db.select(f'SELECT * FROM deltas FINAL WHERE generation={args.generation} AND ({bucket_filter})'))
        # ClickHouse emits Int128 as strings; compare exact integers.
        for row in actual:
            for key in FIELDS:
                row[key] = int(row[key])
        if sorted(encode(row) for row in actual) != sorted(encode(row) for row in plan['deltas']):
            raise RuntimeError('delta generation verification failed')
        boundary('before_commit')
        db.insert('commits', [dict(generation=args.generation, input_json=source,
                                 input_sha256=hashlib.sha256(source.encode()).hexdigest(),
                                 plan_sha256=hashlib.sha256(text.encode()).hexdigest())])
        boundary('after_commit')
        print(encode(dict(generation=args.generation, input_events=len(events), lookup=lookup,
                          changed_identities=len(plan['states']), delta_rows=len(plan['deltas']), verification_bucket_chunk=128,
                          measurements=db.measurements)))


def event(number, kind='purchase', **changes):
    row = dict(event_id=str(uuid.UUID(int=number)), schema_version=1, tenant_id='merchant_a',
               user_id='fixture_user', event_type=kind, occurred_at='2026-11-01T12:00:00Z')
    if kind == 'purchase':
        row.update(order_id='order_' + str(number), currency='USD', amount_cents=100)
    elif kind == 'api_request':
        row['status_code'] = 503
    return row | changes


def reference(batches):
    # Full rebuild oracle, deliberately separate from signed transition logic.
    variants = defaultdict(set)
    for batch in batches:
        for value in batch:
            variants[value['event_id']].add(encode(value))
    totals = defaultdict(lambda: [0]*5)
    for values in variants.values():
        if len(values) != 1:
            continue
        row = json.loads(next(iter(values)))
        minute = datetime.fromisoformat(row['occurred_at'].replace('Z', '+00:00')).replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
        traffic = totals[row['tenant_id'], minute, 1, '']
        traffic[0] += 1
        if row['event_type'] == 'api_request':
            traffic[1] += 1
            traffic[2] += int(row['status_code'] >= 500)
        if row['event_type'] == 'purchase':
            revenue = totals[row['tenant_id'], minute, 2, row['currency']]
            revenue[3] += 1
            revenue[4] += row['amount_cents']
    return {key: tuple(values) for key, values in totals.items() if any(values)}, variants


def verify(db, batches):
    expected, variants = reference(batches)
    head = db.head()
    if db.metrics(head) != expected:
        raise AssertionError('incremental metrics differ from full exact rebuild')
    actual = db.states(variants, head)
    expected_states = {key: (min(values), max(values)) for key, values in variants.items()}
    if actual != expected_states:
        raise AssertionError('full identity min/max strings differ')
    return dict(verified_ids=len(variants), conflicts=sum(len(v) > 1 for v in variants.values()),
                metric_rows=len(expected), generation=head)


def main(args):
    os.chdir(ROOT)
    if args.out and args.out.exists():
        raise ValueError('preserve evidence: choose a new output')
    if args.scales != sorted(set(args.scales)) or any(n < 1000 or n > 1000000 for n in args.scales):
        raise ValueError('use ascending distinct scales between 1000 and 1000000')
    name = 'streamforge-incremental-' + uuid.uuid4().hex[:10]
    private = ROOT / 'load/results/raw' / name
    private.mkdir(mode=0o700, parents=True)
    image = json.loads(ROOT.joinpath('deploy/vendor-images.json').read_text())['clickhouse']
    result = dict(success=False, recorded_at=datetime.now(timezone.utc).isoformat(), image=image,
                  project=name, network='none', clickhouse_memory_bytes=2 << 30, query_memory_bytes=64 << 20,
                  scope='serialized synthetic ClickHouse generations; no Kafka integration, retained migration or SLO claim',
                  aws_operations=False, crash_checks=[], scales=[],
                  experiment_sampling_logs_disabled=['metric_log', 'query_metric_log'],
                  sampling_scope='experiment only; query_log, part_log, native statistics and errors retained')
    created = False
    started = time.perf_counter()
    try:
        result['docker_environment'] = run(['docker', 'info', '--format', 'cpus={{.NCPU}} memory={{.MemTotal}}']).stdout.decode().strip()
        run(['docker', 'run', '-d', '--name', name, '--network', 'none', '--memory', '2g',
             '-v', str(ROOT/'deploy/clickhouse/resources.xml')+':/etc/clickhouse-server/config.d/resources.xml:ro',
             '-v', str(HERE/'sampling.xml')+':/etc/clickhouse-server/config.d/zz-probe-sampling.xml:ro', image])
        created = True
        for _ in range(90):
            probe = subprocess.run(['docker', 'exec', name, 'sh', '-c',
                                    'case "$(cat /proc/1/comm)" in clickhouse*) ;; *) exit 1;; esac; clickhouse-client --query "SELECT 1"'], capture_output=True, timeout=5)
            if probe.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError('owned ClickHouse did not start')

        merged = run(['docker', 'exec', name, 'cat', '/var/lib/clickhouse/preprocessed_configs/config.xml']).stdout
        config = ET.fromstring(merged)
        if any(config.find(tag) is not None for tag in ('metric_log', 'query_metric_log')) or any(config.find(tag) is None for tag in ('query_log', 'part_log')):
            raise RuntimeError('experiment sampling/logging profile did not match the disclosed configuration')
        result['sampling_profile_verified'] = True

        def restart():
            run(['docker', 'restart', name])
            for _ in range(60):
                if subprocess.run(['docker', 'exec', name, 'clickhouse-client', '--query', 'SELECT 1'], capture_output=True, timeout=5).returncode == 0:
                    return
                time.sleep(1)
            raise RuntimeError('owned server restart did not recover')

        def database(label):
            db = Database(name, 'probe_' + label)
            run(['docker', 'exec', name, 'clickhouse-client', '--query', 'CREATE DATABASE ' + db.database])
            db.initialize()
            return db

        def batch_file(label, values):
            path = private / (label + '.json')
            path.write_text(encode(values))
            path.chmod(0o600)
            return path

        def command(db, generation, path, fault=''):
            return [sys.executable, str(HERE/'prototype.py'), 'worker', '--container', name,
                    '--database', db.database, '--generation', str(generation), '--batch', str(path), '--fault', fault]

        def launch(db, generation, path, fault='', code=0, reason=None):
            response = subprocess.run(command(db, generation, path, fault), capture_output=True, text=True, timeout=90)
            (private / (db.database + '_' + str(generation) + '_' + (fault or 'normal') + '_' + uuid.uuid4().hex[:8] + '.log')).write_text(response.stdout+response.stderr)
            if response.returncode != code:
                raise RuntimeError(f'child exit {response.returncode}, expected {code}: {response.stderr[-3000:]}')
            if reason and reason not in response.stderr:
                raise AssertionError('child failed for an unexpected reason: '+response.stderr[-2000:])
            return json.loads(response.stdout) if code == 0 else None

        if args.mode != 'stress':
            db = database('correctness')
            maximum = (1 << 63)-1
            batches = [
                [event(1), event(2, 'api_request'), event(3, 'page_view'), event(4, currency='EUR', amount_cents=250), event(5, amount_cents=maximum)],
                [event(1), event(1), event(6, amount_cents=maximum), event(7, amount_cents=17, occurred_at='2025-01-01T01:01:00Z')],
                [event(1, tenant_id='merchant_b', currency='EUR', amount_cents=999, occurred_at='2026-11-02T10:00:00Z'),
                 event(8, amount_cents=33), event(8, amount_cents=34), event(3, 'page_view', user_id='changed')],
                [event(4, currency='USD', amount_cents=250), event(2, 'api_request', status_code=200),
                 event(9, 'api_request', status_code=200), event(10, 'api_request', status_code=599), event(11, amount_cents=0),
                 event(12, 'page_view', occurred_at='2025-01-01T01:01:00Z')],
                [event(1), event(3, 'page_view'), event(7, amount_cents=17, occurred_at='2025-01-01T01:01:00Z')]]
            files = [batch_file('fixture_' + str(i+1), batch) for i, batch in enumerate(batches)]
            launch(db, 1, files[0])
            verify(db, batches[:1])
            # Repeated child deaths retry the same immutable generation with merges
            # between attempts. Unpublished rows must not replace committed state.
            for fault in ('after_plan', 'after_states', 'after_deltas', 'before_commit'):
                launch(db, 2, files[1], fault, code=86)
                db.sql('OPTIMIZE TABLE states FINAL')
                db.sql('OPTIMIZE TABLE deltas FINAL')
                audit = verify(db, batches[:1])
                result['crash_checks'].append(dict(boundary=fault, visible=audit, exit_code=86))
            # Changed input and generation gaps must fail closed.
            wrong = batch_file('wrong', [event(90)])
            launch(db, 2, wrong, code=2, reason='immutable generation mismatch')
            launch(db, 4, files[3], code=2, reason='generation gap refused')
            verify(db, batches[:1])
            launch(db, 2, files[1], 'after_commit', code=86)
            verify(db, batches[:2])
            if not launch(db, 2, files[1])['already_committed']:
                raise AssertionError('lost commit response did not recover checkpoint')
            launch(db, 2, wrong, code=2, reason='committed input mismatch')
            result['crash_checks'].append(dict(boundary='after_commit', visible=verify(db, batches[:2]), exit_code=86))
            # Test unpublished conflict withdrawal while the old state is merged.
            launch(db, 3, files[2], 'after_deltas', code=86)
            db.sql('OPTIMIZE TABLE states FINAL')
            restart()
            result['unpublished_restart_audit'] = verify(db, batches[:2])
            launch(db, 3, files[2])
            verify(db, batches[:3])
            launch(db, 4, files[3])
            verify(db, batches[:4])
            restart()
            result['restart_audit'] = verify(db, batches[:4])
            # Same-host competing process is rejected by the worker lock.
            first = subprocess.Popen(command(db, 5, files[4], 'hold_after_plan'), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                for _ in range(160):
                    if files[4].with_suffix('.ready').exists():
                        break
                    if first.poll() is not None:
                        raise RuntimeError('lock-holder child exited early')
                    time.sleep(.05)
                else:
                    raise RuntimeError('lock fixture did not become ready')
                competing = subprocess.run(command(db, 5, files[4]), capture_output=True, text=True, timeout=10)
                if competing.returncode != 2 or 'single-writer lock busy' not in competing.stderr:
                    raise AssertionError('same-host writer exclusion failed')
                files[4].with_suffix('.release').write_text('release')
                stdout, stderr = first.communicate(timeout=30)
                if first.returncode:
                    raise RuntimeError(stderr)
                no_op = json.loads(stdout)
                if no_op['changed_identities'] or no_op['delta_rows']:
                    raise AssertionError('identical replay created new state or deltas')
            finally:
                if first.poll() is None:
                    first.kill()
                    first.communicate(timeout=5)
            result['same_host_lock_verified'] = True
            result['no_op_replay'] = no_op
            result['correctness'] = verify(db, batches)
            if db.metrics() != {('merchant_a', '2026-11-01 12:00:00', 1, ''): (5, 2, 1, 0, 0),
                                ('merchant_a', '2026-11-01 12:00:00', 2, 'USD'): (0, 0, 0, 3, 2*maximum),
                                ('merchant_a', '2025-01-01 01:01:00', 1, ''): (2, 0, 0, 0, 0),
                                ('merchant_a', '2025-01-01 01:01:00', 2, 'USD'): (0, 0, 0, 1, 17)}:
                raise AssertionError('hand-calculated final totals differ')
            result['negative_guards'] = ['changed pending input', 'changed committed input', 'generation gap', 'same-host concurrent writer']
            result['physical_rows'] = db.select('SELECT (SELECT count() FROM states) AS identity_rows,(SELECT count() FROM deltas) AS delta_rows')
            result['int128_exact_total_verified'] = str(2*maximum)

            for size in args.scales:
                scale = database('scale_' + str(size))
                # Baseline initialization is explicitly excluded from timed update work.
                identity = 'reinterpretAsUUID(MD5(toString(number)))'
                payload = "toJSONString(map('event_id',toString("+identity+"),'event_type','page_view','occurred_at','2025-01-01T00:00:00Z','tenant_id','history','user_id','fixture'))"
                for offset in range(0, size, 10000):
                    count = min(10000, size-offset)
                    scale.sql(f'INSERT INTO states SELECT {identity},toUInt64(1),{payload},{payload} FROM numbers({offset},{count})' + LIMITS)
                scale.insert('deltas', [dict(tenant='history', minute='2025-01-01 00:00:00', kind=1, currency='', generation=1,
                                              events=size, rpm=0, errors=0, purchases=0, cents=0)])
                scale.insert('commits', [dict(generation=1, input_json='seed', input_sha256='seed', plan_sha256='seed')])
                scale.sql('OPTIMIZE TABLE states FINAL')
                selected = sorted(random.Random(17).sample(range(size), 256))
                keys = scale.select('SELECT toString(reinterpretAsUUID(MD5(toString(number)))) AS event_id FROM '
                                    '(SELECT arrayJoin([' + ','.join(map(str, selected)) + ']) AS number)')
                # Seed payloads have a deterministic map serialization; preserve exact
                # bytes on replay instead of assuming the prototype JSON normalizer.
                prior = scale.states({row['event_id'] for row in keys}, 1)
                replay = [json.loads(prior[row['event_id']][0]) for row in keys[:128]]
                changes = [json.loads(prior[row['event_id']][0]) | dict(tenant_id='conflicting_tenant') for row in keys[128:]]
                fresh = [event((1 << 120) + i, 'page_view', tenant_id='current') for i in range(128)]
                path = batch_file('scale_' + str(size), replay + changes + fresh)
                began = time.perf_counter()
                update = launch(scale, 2, path)
                update_wall = time.perf_counter()-began
                selected_rows = sum(query['rows_read'] for query in update['measurements'])
                if size >= 1000000 and selected_rows >= size//2:
                    raise AssertionError('incremental reads regressed toward a full identity scan')
                if update['changed_identities'] != 256:
                    raise AssertionError('scaling fixture changed unexpected identity count')
                totals = scale.metrics(2)
                expected = {('history', '2025-01-01 00:00:00', 1, ''): (size-128, 0, 0, 0, 0),
                            ('current', '2026-11-01 12:00:00', 1, ''): (128, 0, 0, 0, 0)}
                if totals != expected:
                    raise AssertionError('scale metric totals differ')
                recent = scale.metrics(2, 'current', '2026-11-01 11:45:00', '2026-11-01 12:01:00')
                if recent != {key: value for key, value in expected.items() if key[0] == 'current'}:
                    raise AssertionError('window query differs')
                window = scale.measurements[-1]
                baseline = {}
                try:
                    rows = scale.select("SELECT JSONExtractString(lo,'tenant_id') AS tenant,count() AS events FROM "
                                        '(SELECT event_id,argMax(payload_min,generation) AS lo,argMax(payload_max,generation) AS hi '
                                        'FROM states WHERE generation<=2 GROUP BY event_id) WHERE lo=hi GROUP BY tenant ORDER BY tenant')
                    if {row['tenant']: int(row['events']) for row in rows} != {'current': 128, 'history': size-128}:
                        raise AssertionError('full rebuild oracle differs')
                    baseline = dict(success=True, measurement=scale.measurements[-1])
                except RuntimeError as error:
                    if not re.search(r'Code: (241|159)\.', str(error)):
                        raise
                    baseline = dict(success=False, error=str(error), memory_budget_bytes=64 << 20)
                result['scales'].append(dict(retained_identities=size, source_batch_events=384, existing_lookup_ids=256,
                                             new_ids=128, changed_identities=256, exact_totals_verified=True,
                                             update_wall_seconds=update_wall, total_update_select_rows=selected_rows, incremental=update,
                                             window_query=window, full_identity_rebuild=baseline))
        if args.stress:
            from stress import exercise
            result['stress'] = exercise(database, batch_file, launch, private, restart)
        result['success'] = True
    except Exception as error:
        result['failure'] = str(error)[:2400]
        raise
    finally:
        if created:
            stats = subprocess.run(['docker', 'stats', '--no-stream', '--format', '{{json .}}', name], capture_output=True, text=True, timeout=15)
            (private/'docker-stats.json').write_text(stats.stdout)
            diagnostics = subprocess.run(['docker', 'exec', name, 'clickhouse-client', '--query',
                "SELECT metric,value FROM system.metrics WHERE metric IN ('MemoryTracking','BackgroundMergesAndMutationsPoolTask','Query') FORMAT JSONEachRow"], capture_output=True, text=True, timeout=10)
            (private/'server-memory.jsonl').write_text(diagnostics.stdout+diagnostics.stderr)
            native_logs = subprocess.run(['docker', 'exec', name, 'tail', '-n', '100', '/var/log/clickhouse-server/clickhouse-server.err.log'], capture_output=True, text=True, timeout=10)
            (private/'server-errors.log').write_text(native_logs.stdout+native_logs.stderr)
            logs = subprocess.run(['docker', 'logs', '--tail', '100', name], capture_output=True, text=True)
            (private/'clickhouse.log').write_text(logs.stdout+logs.stderr)
            cleanup = subprocess.run(['docker', 'rm', '-f', '-v', name], capture_output=True, text=True, timeout=60)
            result['cleanup_success'] = cleanup.returncode == 0
            if cleanup.returncode:
                result['success'] = False
        result['elapsed_seconds'] = time.perf_counter()-started
        result['source_revision'] = run(['git', 'rev-parse', 'HEAD']).stdout.decode().strip()
        result['source_dirty'] = bool(run(['git', 'status', '--porcelain']).stdout.strip())
        result['source_sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                   for path in (HERE/'prototype.py', HERE/'schema.sql', HERE/'stress.py', HERE/'sampling.xml')}
        (private/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
        if result['success'] and result.get('cleanup_success') and args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(result, indent=2)+'\n')
    if not result.get('cleanup_success'):
        raise RuntimeError('owned container/volume cleanup failed; see private summary')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    experiment = sub.add_parser('run')
    experiment.add_argument('--out', type=Path)
    experiment.add_argument('--stress', action='store_true', help='also run adversarial parts, hot IDs, buckets and a deliberately unsafe split-brain control')
    experiment.add_argument('--scales', type=int, nargs='+', default=[10000, 100000, 1000000])
    adversarial = sub.add_parser('stress', help='independent adversarial cases, without the compacted-scale baseline')
    adversarial.add_argument('--out', type=Path)
    adversarial.set_defaults(stress=True, scales=[])
    child = sub.add_parser('worker')
    child.add_argument('--container', required=True)
    child.add_argument('--database', required=True)
    child.add_argument('--generation', type=int, required=True)
    child.add_argument('--batch', type=Path, required=True)
    child.add_argument('--fault', default='', choices=['', 'after_plan', 'after_states', 'after_deltas', 'before_commit', 'after_commit', 'hold_after_plan', 'hold_before_plan'])
    options = parser.parse_args()
    try:
        worker(options) if options.mode == 'worker' else main(options)
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
