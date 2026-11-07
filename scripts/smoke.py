"""Real infrastructure checks. Appends a small, uniquely namespaced fixture per run."""
import json
import subprocess
import uuid
from datetime import datetime, timezone

def compose(*args, data=None):
    return subprocess.check_output(['./scripts/compose.sh', *args], input=data, text=True)

def ch(sql):
    return compose('exec', '-T', 'clickhouse', 'sh', '-c',
        'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery', data=sql).strip()

for topic in ['streamforge.events.v1', 'streamforge.events.dlq.v1']:
    description = compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', 'kafka:19092', '--describe', '--topic', topic)
    assert 'PartitionCount: 12' in description and 'ReplicationFactor: 1' in description, description
    print(description.splitlines()[0])
assert compose('exec', '-T', 'postgres', 'psql', '-U', 'streamforge', '-d', 'streamforge', '-Atc',
    "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('tenants','streams','api_keys','replay_jobs','replay_audit')").strip() == '5'
tenant = 'smoke_' + uuid.uuid4().hex
now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
rows = []
for index, (kind, cents, currency, status, seconds) in enumerate([
    ('purchase', 12999, 'USD', None, 1), ('purchase', 250, 'EUR', None, 2),
    ('page_view', 0, '', None, 3), ('api_request', 0, '', 200, 4),
    ('api_request', 0, '', 503, 5), ('page_view', 0, '', None, 61)]):
    from datetime import timedelta
    rows.append(dict(event_id=str(uuid.uuid4()), tenant_id=tenant, user_id='synthetic', session_id='',
        event_type=kind, occurred_at=(now + timedelta(seconds=seconds)).strftime('%Y-%m-%d %H:%M:%S.000'),
        received_at=now.strftime('%Y-%m-%d %H:%M:%S.000'), order_id='order' if kind=='purchase' else '',
        amount_cents=cents, currency=currency, status_code=status, kafka_topic='smoke', kafka_partition=0,
        kafka_offset=index, sink_batch_id=tenant, schema_version=1))
ch('INSERT INTO events_raw FORMAT JSONEachRow\n' + '\n'.join(json.dumps(row) for row in rows))
assert ch(f"SELECT count() FROM events_raw WHERE tenant_id='{tenant}'") == '6'
revenue = ch(f"SELECT currency, sumMerge(purchase_count_state), sumMerge(revenue_cents_state) FROM revenue_1m WHERE tenant_id='{tenant}' GROUP BY currency ORDER BY currency FORMAT JSONEachRow")
assert [list(json.loads(line).values()) for line in revenue.splitlines()] == [['EUR', 1, 250], ['USD', 1, 12999]], revenue
traffic = ch(f"SELECT minute, countMerge(event_count_state) AS events, sumMerge(api_request_count_state) AS rpm, sumMerge(api_error_count_state) AS errors, if(rpm=0,0,errors/rpm) AS error_rate FROM traffic_1m WHERE tenant_id='{tenant}' GROUP BY minute ORDER BY minute FORMAT JSONEachRow")
values = [json.loads(line) for line in traffic.splitlines()]
assert [(v['events'], v['rpm'], v['errors'], v['error_rate']) for v in values] == [(5,2,1,0.5), (1,0,0,0)], traffic
assert values[0]['minute'] == now.strftime('%Y-%m-%d %H:%M:%S')
print('PASS PostgreSQL tables; raw rows=6; revenue EUR=250/USD=12999; minute events=5/1; RPM=2/0; error rates=0.5/0')
print('Fixture tenant:', tenant)
