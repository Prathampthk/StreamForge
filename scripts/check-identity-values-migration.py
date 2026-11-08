#!/usr/bin/env python3
"""Exercise the retained-state migration on disposable local fixtures only."""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

project='streamforge-values-migration-check-'+uuid.uuid4().hex[:8]
compose=['docker','compose','-p',project,'--env-file','.env','-f','deploy/compose.yaml','-f','deploy/compose.ci.yaml']
consumer=project+'-consumer-1'
cmd=compose+['exec','-T','clickhouse','sh','-c','exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery']
def sql(statement,check=True):
 return subprocess.run(cmd,input=statement,text=True,capture_output=True,check=check,timeout=90)
try:
 subprocess.run(compose+['up','-d','--wait','--wait-timeout','180','clickhouse'],check=True,stdout=subprocess.DEVNULL)
 for file in sorted(Path('migrations/clickhouse').glob('00[1-4]_*.sql')):sql(file.read_text())
 at=datetime.now(timezone.utc).isoformat();stamp=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
 events=[];rows=[]
 for i in range(64):
  event=dict(schema_version=1,event_id=str(uuid.uuid5(uuid.NAMESPACE_DNS,'migration-fixture-'+str(i))),tenant_id='migration_fixture',user_id='user',event_type='purchase',occurred_at=at,order_id='order_'+str(i),amount_cents=i+1,currency='USD')
  events.append(event)
  rows.append(event|dict(occurred_at=stamp,received_at=stamp,kafka_topic='fixture',kafka_partition=0,kafka_offset=i,sink_batch_id='fixture',payload_json=json.dumps(event,separators=(',',':'),sort_keys=True)))
 def insert(values):sql('INSERT INTO events_raw FORMAT JSONEachRow\n'+'\n'.join(json.dumps(row) for row in values))
 insert(rows);insert(rows[:2]);changed=events[0]|{'tenant_id':'changed_tenant','amount_cents':999}
 insert([rows[0]|{'tenant_id':'changed_tenant','amount_cents':999,'payload_json':json.dumps(changed,separators=(',',':'),sort_keys=True)}])
 sql(Path('migrations/clickhouse/005_identity_values.sql').read_text())
 insert([rows[1]])
 if sql('SELECT count() FROM event_identity_values FINAL').stdout.strip()!='1':raise RuntimeError('new dual-write view not populated')
 refused=sql(Path('migrations/clickhouse/006_identity_values_cutover.sql').read_text(),False)
 if refused.returncode==0 or 'Retained identities require' not in refused.stderr:raise RuntimeError('unverified retained-data cutover was not correctly refused')
 # A fixture process tests the backfill's stop/restart ownership separately
 # from the Go pipeline, covered by the real-service integration suite.
 subprocess.run(['docker','run','-d','--name',consumer,'--network','none','golang:1.26.10@sha256:cfa27959837144bf4649802ba38e59daffd0dd2a7994aa83daaf113285b0f551','sleep','600'],check=True,stdout=subprocess.DEVNULL)
 with tempfile.TemporaryDirectory(prefix='sf-values-migration-') as directory:
  subprocess.run(['python3','scripts/backfill-identity-values.py','--project',project,'--chunk','128','--directory',directory],check=True)
  state=json.loads((Path(directory)/'progress.json').read_text())
  if not state.get('complete') or state['verified_ids']!=64:raise RuntimeError('fixture backfill did not verify all identities')
  # Idempotent completed invocation must leave running services alone.
  subprocess.run(['python3','scripts/backfill-identity-values.py','--project',project,'--chunk','128','--directory',directory],check=True,stdout=subprocess.DEVNULL)
 subprocess.run(['sh','scripts/migrate-clickhouse.sh'],env=os.environ|{'STREAMFORGE_COMPOSE_PROJECT':project},check=True)
 legacy_before=sql('SELECT count() FROM event_identity_states').stdout.strip()
 insert([rows[2]])
 if sql('SELECT count() FROM event_identity_states').stdout.strip()!=legacy_before:raise RuntimeError('legacy writes did not stop')
 actual=sql('SELECT count(),sum(toInt128(amount_cents)) FROM events_canonical FORMAT TabSeparated').stdout.strip()
 if actual!='63\t2079':raise RuntimeError('canonical fixture totals incorrect: '+actual)
 if sql('SELECT count() FROM events_raw').stdout.strip()!='69':raise RuntimeError('raw evidence changed')
 if sql('SELECT count() FROM event_identity_conflicts').stdout.strip()!='1':raise RuntimeError('conflict missing')
 if sql('SELECT count() FROM event_identity_states').stdout.strip()=='0':raise RuntimeError('legacy states removed')
 print('PASS disposable migration guard, exact min/max copy, checkpoint, cutover, conflict withdrawal, raw/legacy preservation and repeat invocation')
finally:
 subprocess.run(['docker','rm','-f',consumer],capture_output=True)
 subprocess.run(compose+['down','--volumes','--remove-orphans'],check=True,stdout=subprocess.DEVNULL)
