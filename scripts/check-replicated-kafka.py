#!/usr/bin/env python3
"""Own an isolated RF3/minISR2 Kafka process-failure drill; never use AWS.

Only this run's brokers/volumes are destroyed. If the retained local ClickHouse
is running, pause its derived refreshes outside any benchmark and restore them.
No existing Kafka topic, facts or identity data are altered.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--with-process-shutdown', action='store_true', help='also test real ingestion, consumer and query SIGTERM using fresh databases')
    a = parser.parse_args()
    os.chdir(Path(__file__).resolve().parent.parent)
    if a.out and a.out.exists():
        parser.error('preserve earlier evidence; choose a new output')
    project = 'streamforge-rf3-' + uuid.uuid4().hex[:10]
    directory = Path('load/results/raw') / project
    directory.mkdir(parents=True, mode=0o700)
    env = os.environ | {'LOCAL_UID': str(os.getuid()), 'LOCAL_GID': str(os.getgid()), 'RF3_RESULTS_DIR': str(directory.resolve()), 'RF3_DB_PASSWORD': secrets.token_urlsafe(24), 'RUN_PROCESS_SHUTDOWN': '1' if a.with_process_shutdown else '0'}
    compose = ['docker', 'compose', '-p', project, '-f', 'deploy/compose.kafka-replicated.yaml']
    client = project + '-client'
    shutdown_client = project + '-shutdown'
    active = []
    started = time.monotonic()
    cleanup = []

    def run(command, **kwargs):
        return subprocess.run(command, env=env, check=True, capture_output=True, timeout=kwargs.pop('timeout', 60), **kwargs)

    def sql(statement):
        command = ['./scripts/compose.sh', 'exec', '-T', 'clickhouse', 'sh', '-c',
                   'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" '
                   '--database streamforge --multiquery']
        return run(command, input=statement.encode()).stdout.decode()

    def kafka(broker, tool, *args):
        return run(compose + ['exec', '-T', 'broker'+str(broker), '/opt/kafka/bin/'+tool+'.sh',
                              '--bootstrap-server', 'localhost:19092', *args]).stdout.decode()

    def config(broker, minimum):
        kafka(broker, 'kafka-configs', '--alter', '--entity-type', 'topics', '--entity-name',
              'streamforge.events.v1', '--add-config', 'min.insync.replicas='+str(minimum))
        description = kafka(broker, 'kafka-configs', '--describe', '--entity-type', 'topics',
                            '--entity-name', 'streamforge.events.v1')
        if 'min.insync.replicas='+str(minimum) not in description:
            raise RuntimeError('topic ISR override not confirmed')
        return description.strip()

    def wait(name, container=None):
        container = container or client
        deadline = time.monotonic()+100
        while time.monotonic() < deadline:
            path = directory / name
            if path.exists():
                return json.loads(path.read_text())
            state = run(['docker', 'inspect', '--format', '{{.State.Running}} {{.State.ExitCode}}', container]).stdout.decode().strip()
            if state != 'true 0':
                logs = subprocess.run(['docker', 'logs', container], capture_output=True, text=True).stdout
                (directory / (container+'.log')).write_text(logs)
                raise RuntimeError('drill exited before '+name+': '+logs[-3500:])
            time.sleep(.5)
        raise RuntimeError('timeout waiting for '+name)

    def release(name):
        temporary = directory / (name+'.tmp')
        temporary.write_text('{}\n')
        temporary.chmod(0o600)
        temporary.replace(directory / name)

    result = {'success': False, 'project': project, 'recorded_at': datetime.now(timezone.utc).isoformat(),
              'scope': 'three combined broker/controller processes on one Docker VM; SIGKILL with retained disks',
              'replication_factor': 3, 'normal_min_isr': 2, 'negative_min_isr': 3,
              'partitions': 12, 'configured_max_batch_events': 200, 'positive_phase_events': 120,
              'phases': {}, 'aws_operations': False}
    try:
        result['docker_environment'] = run(['docker', 'info', '--format', 'cpus={{.NCPU}} memory={{.MemTotal}}'], timeout=15).stdout.decode().strip()
        for volume in ['streamforge_go_mod', 'streamforge_go_build']:
            run(['docker', 'volume', 'create', volume])
        running = subprocess.run(['docker', 'inspect', '--format', '{{.State.Running}}',
                                  'streamforge-clickhouse-1'], capture_output=True, text=True)
        if running.returncode == 0 and running.stdout.strip() == 'true':
            active = sql("SELECT view FROM system.view_refreshes WHERE database='streamforge' AND status IN ('Running','Scheduled')").splitlines()
            if any(not re.fullmatch(r'(identity_metrics_[0-9]{2}|revenue_1m|traffic_1m)_refresh', v) for v in active):
                raise RuntimeError('unexpected retained refresh; refusing maintenance')
            for v in active:
                sql('SYSTEM STOP VIEW '+v)
            for _ in range(60):
                if sql("SELECT count() FROM system.view_refreshes WHERE database='streamforge' AND status='Running'").strip() == '0':
                    break
                time.sleep(.5)
            else:
                raise RuntimeError('retained refreshes did not settle')
        result['retained_refreshes_temporarily_paused'] = active
        run(['./scripts/go.sh', 'go', 'test', '-c', '-race', '-tags=integration', '-o',
             '/src/'+str(directory/'rf3.test'), './tests/integration'], timeout=300)
        if a.with_process_shutdown:
            for service in ['ingest-api', 'consumer', 'query-api']:
                run(['./scripts/go.sh', 'go', 'build', '-race', '-o', '/src/'+str(directory/service), './cmd/'+service], timeout=300)
        result['test_binary_sha256'] = hashlib.sha256((directory/'rf3.test').read_bytes()).hexdigest()
        run(compose+['up', '-d', '--wait', '--wait-timeout', '240', 'broker1', 'broker2', 'broker3'], timeout=270)
        kafka(1, 'kafka-topics', '--create', '--topic', 'streamforge.events.v1', '--partitions', '12',
              '--replication-factor', '3', '--config', 'min.insync.replicas=2', '--config',
              'unclean.leader.election.enable=false', '--config', 'retention.ms=3600000')
        result['initial_topic_config'] = config(1, 2)
        run(compose+['run', '-d', '--no-deps', '--name', client, 'drill'])
        baseline = wait('baseline.json')
        result['phases']['baseline'] = baseline
        first = int(baseline['leaders']['0'])
        result['first_killed_broker'] = first
        survivor = next(i for i in (1, 2, 3) if i != first)
        result['negative_topic_config'] = config(survivor, 3)
        run(['docker', 'kill', '--signal', 'KILL', project+'-broker'+str(first)+'-1'])
        release('reject.go')
        result['phases']['min_isr_rejection'] = wait('rejected.json')
        result['restored_topic_config'] = config(survivor, 2)
        release('one-down.go')
        result['phases']['one_down'] = wait('one-down.json')
        run(['docker', 'start', project+'-broker'+str(first)+'-1'])
        release('restored.go')
        result['phases']['restored'] = wait('restored.json')
        second = next(i for i in (1, 2, 3) if i != first)
        result['second_killed_broker'] = second
        run(['docker', 'kill', '--signal', 'KILL', project+'-broker'+str(second)+'-1'])
        release('audit.go')
        audit = wait('result.json')
        code = run(['docker', 'wait', client], timeout=30).stdout.decode().strip()
        if code != '0' or not audit['success'] or audit['accepted'] != 360 or audit['verified_exact_payloads'] != 360:
            raise RuntimeError('drill did not pass complete reconciliation')
        if a.with_process_shutdown:
            run(compose+['--profile', 'shutdown', 'up', '-d', '--wait', '--wait-timeout', '120', 'postgres', 'clickhouse'], timeout=150)
            # pg_isready/SELECT 1 may briefly see initialization daemons. Wait
            # for PID 1 to be the final service before applying any migrations.
            for service, final in [('postgres', 'postgres'), ('clickhouse', 'clickhouse')]:
                for _ in range(60):
                    pid = run(compose+['exec', '-T', service, 'cat', '/proc/1/comm']).stdout.decode().strip()
                    if pid.startswith(final):
                        break
                    time.sleep(.5)
                else:
                    raise RuntimeError(service+' final daemon did not start')
            for migration in sorted(Path('migrations/postgres').glob('*.up.sql')):
                run(compose+['exec', '-T', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'streamforge', '-d', 'streamforge'], input=migration.read_bytes())
            for migration in sorted(Path('migrations/clickhouse').glob('*.sql')):
                run(compose+['exec', '-T', 'clickhouse', 'sh', '-c',
                    'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery'], input=migration.read_bytes())
            run(compose+['run', '-d', '--no-deps', '--name', shutdown_client, 'drill', '/results/rf3.test',
                         '-test.v', '-test.run=^Test(Ingest|Consumer|Query)SIGTERM$', '-test.timeout=4m'])
            result['ingest_shutdown'] = wait('ingest-shutdown.json', shutdown_client)
            run(['docker', 'start', project+'-broker'+str(second)+'-1'])
            release('consumer-ready.go')
            result['consumer_shutdown'] = wait('consumer-shutdown.json', shutdown_client)
            result['query_shutdown'] = wait('query-shutdown.json', shutdown_client)
            code = run(['docker', 'wait', shutdown_client], timeout=30).stdout.decode().strip()
            if code != '0':
                raise RuntimeError('real-process shutdown suite failed')
        result.update(success=True, audit=audit, elapsed_seconds=round(time.monotonic()-started, 3))
    finally:
        logs = subprocess.run(['docker', 'logs', client], capture_output=True, text=True)
        (directory/'client.log').write_text(logs.stdout+logs.stderr)
        shutdown_logs = subprocess.run(['docker', 'logs', shutdown_client], capture_output=True, text=True)
        (directory/'shutdown.log').write_text(shutdown_logs.stdout+shutdown_logs.stderr)
        if a.with_process_shutdown:
            diagnostics = subprocess.run(['docker', 'logs', '--tail', '200', project+'-clickhouse-1'], capture_output=True, text=True)
            (directory/'clickhouse.log').write_text(diagnostics.stdout+diagnostics.stderr)
        subprocess.run(['docker', 'rm', '-f', client, shutdown_client], capture_output=True)
        try:
            run(compose+['--profile', 'shutdown', 'down', '--volumes', '--remove-orphans'], timeout=90)
        except Exception as error:
            cleanup.append('owned Kafka cleanup: '+type(error).__name__)
        for v in active:
            try:
                sql('SYSTEM START VIEW '+v)
            except Exception as error:
                cleanup.append('refresh restore '+v+': '+type(error).__name__)
        for binary in ['rf3.test', 'ingest-api', 'consumer', 'query-api']:
            (directory/binary).unlink(missing_ok=True)
        result['cleanup_errors'] = cleanup
        if cleanup:
            result['success'] = False
        result['source_revision'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        result['source_dirty'] = bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True).strip())
        result['source_sha256'] = {str(f): hashlib.sha256(f.read_bytes()).hexdigest() for f in
            [Path('scripts/check-replicated-kafka.py'), Path('tests/integration/replicated_kafka_test.go'), Path('deploy/compose.kafka-replicated.yaml'), Path('tests/integration/shutdown_process_test.go')]}
        (directory/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
        if result['success'] and not cleanup and a.out:
            a.out.parent.mkdir(parents=True, exist_ok=True)
            a.out.write_text(json.dumps(result, indent=2)+'\n')
        if cleanup:
            raise RuntimeError('cleanup or schedule restoration failed; see '+str(directory))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
