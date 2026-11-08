#!/usr/bin/env python3
"""Local-only, bounded, exact-verified identity migration; never deletes source facts."""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import time
import uuid

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--project',default='streamforge')
p.add_argument('--chunk',type=int,default=8192)
p.add_argument('--directory',type=Path,default=Path('load/results/raw/identity-values-backfill'))
a=p.parse_args()
if not 128<=a.chunk<=8192:p.error('chunk must be 128..8192')
if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}',a.project):p.error('invalid local Compose project')
a.directory.mkdir(parents=True,exist_ok=True)
checkpoint=a.directory/'progress.json'
state=json.loads(checkpoint.read_text()) if checkpoint.exists() else {'partitions':{},'verified_ranges':0,'verified_ids':0}
cmd=['docker','compose','-p',a.project,'--env-file','.env','-f','deploy/compose.yaml','exec','-T','clickhouse','sh','-c','exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database streamforge --multiquery']
if state.get('complete'):
 print(json.dumps(state));raise SystemExit(0)
context=json.loads(subprocess.check_output(['docker','context','inspect'],text=True))[0]
endpoint=os.environ.get('DOCKER_HOST') or context['Endpoints']['docker']['Host']
if not endpoint.startswith('unix://'):raise RuntimeError('this script requires a local Unix-socket Docker engine')
consumer=a.project+'-consumer-1'
running=subprocess.check_output(['docker','inspect','--format','{{.State.Running}}',consumer],text=True).strip()=='true'
settings='max_threads=1,max_final_threads=1,max_block_size=1024,max_memory_usage=268435456,max_execution_time=30'
def sql(statement):
 r=subprocess.run(cmd,input=statement,text=True,capture_output=True,timeout=90)
 if r.returncode:raise RuntimeError(r.stderr[:1600])
 return r.stdout
def save():
 temporary=checkpoint.with_suffix('.tmp');temporary.write_text(json.dumps(state,indent=2)+'\n');temporary.replace(checkpoint)
started=time.monotonic();cutover=False
try:
 sql(Path('migrations/clickhouse/005_identity_values.sql').read_text())
 identities={r['name']:r['uuid'] for r in (json.loads(line) for line in sql("SELECT name,toString(uuid) AS uuid FROM system.tables WHERE database='streamforge' AND name IN ('event_identity_states','event_identity_values') FORMAT JSONEachRow").splitlines())}
 if 'table_uuids' in state and state['table_uuids']!=identities:raise RuntimeError('checkpoint belongs to different source/target tables')
 state['table_uuids']=identities;save()
 if running:subprocess.run(['docker','stop','--time','20',consumer],check=True,stdout=subprocess.DEVNULL)
 sql('SYSTEM STOP VIEW revenue_1m_refresh; SYSTEM STOP VIEW traffic_1m_refresh; SYSTEM STOP MERGES event_identity_states; SYSTEM STOP MERGES events_raw;')
 for _ in range(120):
  health=[json.loads(x) for x in sql("SELECT status FROM system.view_refreshes WHERE database='streamforge' FORMAT JSONEachRow").splitlines()]
  if len(health)==2 and all(x['status']=='Disabled' for x in health):break
  time.sleep(.5)
 else:raise RuntimeError('refreshes did not settle')
 for _ in range(120):
  active=int(sql("SELECT count() FROM system.merges WHERE database='streamforge' AND table IN ('event_identity_states','events_raw')").strip())
  if active==0:break
  time.sleep(.5)
 else:raise RuntimeError('source merges did not settle')
 for partition in range(16):
  entry=state['partitions'].setdefault(str(partition),{'last':None,'complete':False})
  if entry['complete']:continue
  while True:
   after=f" AND event_id>toUUID('{uuid.UUID(entry['last'])}')" if entry['last'] else ''
   ids=sql(f'SELECT event_id FROM event_identity_states FINAL WHERE cityHash64(event_id)%16={partition}{after} ORDER BY event_id LIMIT {a.chunk} SETTINGS {settings} FORMAT TabSeparated').split()
   if not ids:
    entry['complete']=True;save();break
   end=str(uuid.UUID(ids[-1]));where=f"cityHash64(event_id)%16={partition}{after} AND event_id<=toUUID('{end}')"
   # Read/copy every physical min/max state in the inclusive range, not just
   # a LIMIT of physical rows that could cut an identity's variants in half.
   body=f"INSERT INTO event_identity_values SELECT event_id,finalizeAggregation(payload_min),finalizeAggregation(payload_max) FROM event_identity_states WHERE {where} SETTINGS {settings},min_insert_block_size_rows=8192,min_insert_block_size_bytes=1048576,max_insert_block_size=8192;\n"
   body+=f"SELECT 'old' AS source,event_id,minMerge(payload_min) AS lo,maxMerge(payload_max) AS hi FROM event_identity_states WHERE {where} GROUP BY event_id ORDER BY event_id SETTINGS {settings} FORMAT JSONEachRow;\n"
   body+=f"SELECT 'new' AS source,event_id,payload_min AS lo,payload_max AS hi FROM event_identity_values FINAL WHERE {where} ORDER BY event_id SETTINGS {settings} FORMAT JSONEachRow;"
   old=[];new=[]
   for line in sql(body).splitlines():
    row=json.loads(line);side=row.pop('source');(old if side=='old' else new).append(row)
   if len(old)!=len(ids) or old!=new:raise RuntimeError(f'exact state verification failed in partition {partition}; no checkpoint advanced')
   entry['last']=end;state['verified_ranges']+=1;state['verified_ids']+=len(old);save()
   if state['verified_ranges']%32==0:print(json.dumps({'partition':partition,'verified_ids':state['verified_ids'],'seconds':round(time.monotonic()-started,1)}),flush=True)
   if state['verified_ranges']%16==0:sql('SYSTEM JEMALLOC PURGE;')
 # Count identities without reading opaque string states; no per-ID hash is
 # substituted for the exact payload comparisons above.
 counts=[]
 for table in ['event_identity_states','event_identity_values']:
  counts.append(int(sql(f'SELECT count() FROM (SELECT event_id FROM {table} FINAL) SETTINGS {settings}').strip()))
 if counts[0]!=counts[1]:raise RuntimeError(f'total identity counts differ: {counts}')
 if state['verified_ids']!=counts[0]:raise RuntimeError('source cardinality changed; repeat verification with a fresh progress directory')
 sql(f'INSERT INTO identity_values_migration VALUES (1,{counts[0]},now64(3));')
 sql(Path('migrations/clickhouse/006_identity_values_cutover.sql').read_text())
 cutover=True
 state.update(complete=True,cutover=True,source_ids=counts[0],target_ids=counts[1],elapsed_seconds=round(time.monotonic()-started,3));save()
 print(json.dumps(state),flush=True)
finally:
 # Legacy states remain on disk. Keep their now-retired merge work paused
 # after cutover; no legacy data is dropped or truncated.
 try:
  if not cutover:sql('SYSTEM START MERGES event_identity_states;')
  sql('SYSTEM START MERGES events_raw; SYSTEM START VIEW revenue_1m_refresh; SYSTEM START VIEW traffic_1m_refresh;')
 finally:
  if running:subprocess.run(['docker','start',consumer],check=True,stdout=subprocess.DEVNULL)
