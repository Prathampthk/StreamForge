"""Adversarial fixture extension. Includes an EXPECTED UNSAFE split-brain control.

A passing exercise confirms the counterexample, not production concurrency
safety. No retained tables, production workers, Kafka or AWS are contacted.
"""
from datetime import datetime, timedelta, timezone
import json
import subprocess
import sys
import time

import prototype as p


def parts(db):
    return db.select("SELECT table,count() AS parts,sum(rows) AS rows,sum(bytes_on_disk) AS bytes "
                     "FROM system.parts WHERE active AND database=" + p.literal(db.database) + ' GROUP BY table ORDER BY table')


def pause_merges(db):
    for table in ('states', 'deltas', 'plans', 'commits'):
        db.sql('SYSTEM STOP MERGES ' + table)


def merge(db):
    for table in ('states', 'deltas', 'plans', 'commits'):
        db.sql('SYSTEM START MERGES ' + table)
        db.sql('OPTIMIZE TABLE ' + table + ' FINAL')


def audit(db, batches, head):
    expected, variants = p.reference(batches[:head])
    if db.metrics(head) != expected:
        raise AssertionError('pinned metric snapshot differs at generation ' + str(head))
    actual = db.states(variants, head)
    if actual != {key: (min(value), max(value)) for key, value in variants.items()}:
        raise AssertionError('pinned identity snapshot differs at generation ' + str(head))
    return dict(generation=head, ids=len(variants), metric_rows=len(expected))


def exercise(database, batch_file, launch, private, restart):
    started = time.perf_counter()
    result = dict(success=False, production_concurrency_safe=False,
                  scope='independent adversarial cases with planned server restarts; expected unsafe split-brain controls included',
                  case_isolation='fresh container; planned restarts before hot-ID, bucket and each ownership case')
    def checkpoint(stage):
        result['last_completed_stage'] = stage
        (private/'stress-progress.json').write_text(json.dumps(result, indent=2)+'\n')

    # 128k IDs distributed across 64 overlapping UUID-range parts.
    db = database('fragmented')
    pause_merges(db)
    size, chunk = 128000, 2000
    identity = 'reinterpretAsUUID(MD5(toString(number)))'
    payload = "toJSONString(map('event_id',toString("+identity+"),'event_type','page_view','occurred_at','2025-01-01T00:00:00Z','tenant_id','history','user_id','fixture'))"
    for offset in range(0, size, chunk):
        db.sql(f'INSERT INTO states SELECT {identity},toUInt64(1),{payload},{payload} FROM numbers({offset},{chunk})' + p.LIMITS)
    db.insert('deltas', [dict(tenant='history', minute='2025-01-01 00:00:00', kind=1, currency='', generation=1,
                              events=size, rpm=0, errors=0, purchases=0, cents=0)])
    db.insert('commits', [dict(generation=1, input_json='seed', input_sha256='seed', plan_sha256='seed')])
    before_parts = parts(db)
    if next(row['parts'] for row in before_parts if row['table'] == 'states') != 64:
        raise AssertionError('fragmentation fixture did not hold 64 state parts')
    keys = db.select('SELECT toString(reinterpretAsUUID(MD5(toString(number)))) AS event_id '
                     'FROM (SELECT arrayJoin([' + ','.join(str(i*499) for i in range(256)) + ']) AS number)')
    ids = {row['event_id'] for row in keys}
    original = db.states(ids, 1)
    seed_lookup = db.measurements[-1]
    values = [json.loads(original[row['event_id']][0]) for row in keys]
    changes = values[:128] + [value | dict(tenant_id='conflict') for value in values[128:]]
    changes += [p.event((1 << 120)+i, 'page_view', tenant_id='current') for i in range(128)]
    update = launch(db, 2, batch_file('fragmented_update', changes))
    expected = {('history', '2025-01-01 00:00:00', 1, ''): (size-128, 0, 0, 0, 0),
                ('current', '2026-11-01 12:00:00', 1, ''): (128, 0, 0, 0, 0)}
    if db.metrics(2) != expected:
        raise AssertionError('fragmented fixture update totals differ')
    unmerged_worker_parts = parts(db)
    if db.states(ids, 1) != original:
        raise AssertionError('new generation changed the pinned source strings')
    before_lookup = db.measurements[-1]
    merge(db)
    if db.states(ids, 1) != original or db.metrics(2) != expected:
        raise AssertionError('owned merges changed pinned identities or current metrics')
    # Repeat after the metric audit so the measurement identifies this lookup.
    db.states(ids, 1)
    after_lookup = db.measurements[-1]
    after_parts = parts(db)
    if next(row['parts'] for row in after_parts if row['table'] == 'states') != 1:
        raise AssertionError('owned state compaction did not produce one part')
    result['parts'] = dict(seed_identities=size, selected_ids=256, before_parts=before_parts,
                           after_parts=after_parts, seed_lookup=seed_lookup, unmerged_worker_parts=unmerged_worker_parts,
                           before_lookup=before_lookup, after_lookup=after_lookup,
                           exact_strings_verified=True, update=update)
    checkpoint('parts')

    # Eight hot IDs change their exact lexicographic maximum in every generation.
    # One new page view per generation keeps the same bucket accumulating deltas.
    restart()  # Independent case: do not carry allocator/cache pressure forward.
    hot = database('hot_history')
    pause_merges(hot)
    batches, samples = [], []
    for generation in range(1, 33):
        values = [p.event(i, tenant_id='hot', attributes={'sequence': f'{generation:06d}'}) for i in range(1, 9)]
        values += [p.event((1 << 120)+generation, 'page_view', tenant_id='hot')]
        batches.append(values)
        update = launch(hot, generation, batch_file('hot_' + str(generation), values))
        if update['changed_identities'] != 9:
            raise AssertionError('hot ID extreme failed to change')
        if generation > 2 and update['delta_rows'] != 1:
            raise AssertionError('already-conflicted hot IDs changed analytical counts')
        if generation in (1, 2, 8, 16, 32):
            proof = audit(hot, batches, generation)
            hot.states({value['event_id'] for value in values[:8]}, generation)
            lookup = hot.measurements[-1]
            hot.metrics(generation, 'hot', '2026-11-01 12:00:00', '2026-11-01 12:01:00')
            samples.append(dict(generation=generation, audit=proof, update=update, hot_lookup=lookup,
                                bucket_query=hot.measurements[-1], parts=parts(hot)))
    hot_ids = {value['event_id'] for value in batches[0][:8]}
    in_ids = ','.join(p.literal(value) for value in sorted(hot_ids))
    before_versions = hot.select('SELECT count() AS versions FROM states WHERE event_id IN (' + in_ids + ')')[0]['versions']
    merge(hot)
    after_versions = hot.select('SELECT count() AS versions FROM states WHERE event_id IN (' + in_ids + ')')[0]['versions']
    if before_versions != 256 or after_versions != 256:
        raise AssertionError('generation-preserving merges unexpectedly discarded history')
    restart()
    pinned = [audit(hot, batches, head) for head in (1, 2, 8, 16, 32)]
    expected_hot = {('hot', '2026-11-01 12:00:00', 1, ''): (32, 0, 0, 0, 0)}
    if hot.metrics(32) != expected_hot:
        raise AssertionError('hot conflicts contributed after withdrawal')
    hot.metrics(32, 'hot', '2026-11-01 12:00:00', '2026-11-01 12:01:00')
    result['hot_history'] = dict(generations=32, hot_ids=8, samples=samples,
                                hot_versions_before_merge=before_versions, hot_versions_after_merge=after_versions,
                                merged_bucket_query=hot.measurements[-1], pinned_after_merge_and_restart=pinned,
                                physical_parts=parts(hot),
                                journal_bytes=hot.select('SELECT (SELECT sum(length(input_json)+length(plan_json)) FROM plans) AS plan_bytes,(SELECT sum(length(input_json)) FROM commits) AS commit_input_bytes')[0])
    checkpoint('hot_history')

    # A source batch spans 32 tenants x 32 minutes x traffic/revenue buckets.
    restart()
    buckets = database('many_buckets')
    values = []
    base = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(1024):
        values.append(p.event(i+1, tenant_id='bucket_' + str(i % 32), amount_cents=i+1,
                              currency='USD' if i % 2 == 0 else 'EUR',
                              occurred_at=(base+timedelta(minutes=i//32)).strftime('%Y-%m-%dT%H:%M:%SZ')))
    first = launch(buckets, 1, batch_file('many_buckets_1', values))
    proof = audit(buckets, [values], 1)
    if proof['metric_rows'] != 2048 or first['delta_rows'] != 2048:
        raise AssertionError('many-bucket fixture did not exercise 2048 metric keys')
    # A narrow tenant/minute read must remain exact amid many unrelated buckets.
    expected, _ = p.reference([values])
    stop = '2026-11-01 12:01:00'
    one = buckets.metrics(1, 'bucket_0', '2026-11-01 12:00:00', stop)
    expected_one = {key: value for key, value in expected.items() if key[0] == 'bucket_0' and key[1] < stop}
    if one != expected_one:
        raise AssertionError('narrow tenant/minute result differs')
    narrow = buckets.measurements[-1]
    # Conflicts in a second batch must withdraw every old tenant/time/currency.
    conflicts = [value | dict(tenant_id='withdrawn', amount_cents=99999) for value in values[::8]]
    second = launch(buckets, 2, batch_file('many_buckets_2', conflicts))
    result['many_buckets'] = dict(first_update=first, second_update=second,
                                  published_audit=audit(buckets, [values, conflicts], 2), narrow_query=narrow,
                                  original_metric_rows=proof['metric_rows'])
    checkpoint('many_buckets')

    # Deliberately bypass the same-directory lock using two private source roots,
    # like two hosts with independent local disks. Both read head=0 and no plan,
    # then A publishes; B resumes with its stale computation for generation=1.
    def unsafe_schedule(label, inputs, expected_b_exit):
        restart()
        unsafe = database(label)
        pause_merges(unsafe)  # Hold both staged identity variants for the failure control.
        children = []
        try:
            paths = []
            for i, values in enumerate(inputs):
                directory = private / (label + '_writer_' + str(i))
                directory.mkdir(mode=0o700)
                path = directory/'batch.json'
                path.write_text(p.encode(values))
                path.chmod(0o600)
                paths.append(path)
                command = [sys.executable, str(p.HERE/'prototype.py'), 'worker', '--container', unsafe.container,
                           '--database', unsafe.database, '--generation', '1', '--batch', str(path), '--fault', 'hold_before_plan']
                children.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
                for _ in range(200):
                    if path.with_suffix('.ready').exists():
                        break
                    if children[-1].poll() is not None:
                        stdout, stderr = children[-1].communicate(timeout=5)
                        raise RuntimeError('split-brain child exited before deterministic barrier: '+(stdout+stderr)[-1800:])
                    time.sleep(.05)
                else:
                    raise RuntimeError('split-brain setup timed out')
            # A wins first and a reader pins generation one.
            paths[0].with_suffix('.release').write_text('release')
            out_a, err_a = children[0].communicate(timeout=30)
            if children[0].returncode:
                raise RuntimeError(err_a)
            head = unsafe.head()
            published_before = unsafe.metrics(head)
            expected_a, _ = p.reference([inputs[0]])
            if head != 1 or published_before != expected_a:
                raise AssertionError('owner A did not publish expected snapshot')
            paths[1].with_suffix('.release').write_text('release')
            out_b, err_b = children[1].communicate(timeout=30)
            if children[1].returncode != expected_b_exit:
                raise RuntimeError('unexpected stale-worker exit: '+err_b)
            if expected_b_exit == 2 and 'identity generation verification failed' not in err_b:
                raise AssertionError('conflicting stale writer failed for wrong reason')
            after = unsafe.metrics(head)
            expected_both = p.reference([inputs[0]])[0] | p.reference([inputs[1]])[0]
            if after == published_before or after != expected_both or unsafe.head() != 1:
                raise AssertionError('expected unsafe stale-writer publication was not reproduced')
            conflicting_commits = unsafe.select('SELECT count() AS rows,uniqExact(input_json) AS distinct_inputs FROM commits WHERE generation=1')[0]
            if conflicting_commits['distinct_inputs'] != (2 if expected_b_exit == 0 else 1):
                raise AssertionError('split-brain commit conflict was not exposed')
            (private/(label+'-writer-a.log')).write_text(out_a+err_a)
            (private/(label+'-writer-b.log')).write_text(out_b+err_b)
            return dict(expected_unsafe=True, counterexample_reproduced=True,
                                              first_worker_exit_code=0, stale_worker_exit_code=expected_b_exit, pinned_generation=head,
                                              exact_union_reference_rows=len(p.reference(inputs)[0]),
                                              pinned_metric_rows_before=len(published_before), pinned_metric_rows_after=len(after),
                                              conflicting_commit_records=conflicting_commits,
                                              conclusion='local locks and max(generation) do not fence independent writers')
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=5)

    result['unsafe_ownership'] = unsafe_schedule('unsafe_ownership',
        [[p.event(1, tenant_id='owner_a')], [p.event(2, tenant_id='owner_b', currency='EUR', amount_cents=200)]], 0)
    result['unsafe_global_conflict'] = unsafe_schedule('unsafe_conflict',
        [[p.event(1, tenant_id='owner_a')], [p.event(1, tenant_id='owner_b', currency='EUR', amount_cents=200)]], 2)
    result.update(success=True, elapsed_seconds=time.perf_counter()-started)
    checkpoint('complete_expected_unsafe_control')
    return result
