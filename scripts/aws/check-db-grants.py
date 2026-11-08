#!/usr/bin/env python3
"""Verify prepared grants in a disposable local ClickHouse instance, never benchmark data."""
import base64,hashlib,json,os,secrets,subprocess,time,uuid
from datetime import datetime,timezone
from urllib.error import HTTPError,URLError
from urllib.request import Request,urlopen
from pathlib import Path
name='streamforge-phase6-grants';password=secrets.token_urlsafe(32);suffix=uuid.uuid4().hex[:12];query='phase6_query_'+suffix;consumer='phase6_consumer_'+suffix
# This dedicated fixed name fails if already owned by another process; never replace it.
command=['docker','run','--rm','-d','--name',name,'--memory','1g','--tmpfs','/var/lib/clickhouse:rw,size=256m','-p','127.0.0.1:18123:8123','-e','CLICKHOUSE_USER','-e','CLICKHOUSE_PASSWORD','-e','CLICKHOUSE_DB=streamforge','-e','CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1','-v',str(Path('deploy/clickhouse/resources.xml').resolve())+':/etc/clickhouse-server/config.d/resources.xml:ro','clickhouse/clickhouse-server:25.8.4.13@sha256:c48c1ed0a54e6e9fde03750860aa86de1e6f40b6fe33c6df540738867b4f6809']
subprocess.run(command,env=os.environ|{'CLICKHOUSE_USER':'streamforge','CLICKHOUSE_PASSWORD':password},check=True,stdout=subprocess.DEVNULL)
def auth(user):return 'Basic '+base64.b64encode((user+':'+password).encode()).decode()
class QueryFailure(RuntimeError):pass
def sql(statement,user='streamforge'):
 req=Request('http://127.0.0.1:18123/?database=streamforge&wait_end_of_query=1&max_execution_time=3',data=statement.encode(),headers={'Authorization':auth(user)})
 try:
  with urlopen(req,timeout=5) as r:return r.read(65536).decode()
 except HTTPError as error:raise QueryFailure(error.read().decode().split('\n',1)[0]) from None
def denied(statement,user):
 try:sql(statement,user)
 except QueryFailure as error:
  if 'Code: 497.' not in str(error):raise RuntimeError('negative check failed for reason other than privileges') from None
  return
 raise RuntimeError('unexpectedly permitted operation')
try:
 config='<config><user>streamforge</user><password>'+password+'</password></config>'
 subprocess.run(['docker','exec','-i',name,'sh','-c','umask 077; cat > /tmp/check-client.xml'],input=config.encode(),check=True)
 for _ in range(60):
  ready=subprocess.run(['docker','exec',name,'clickhouse-client','--config-file','/tmp/check-client.xml','--query','SELECT 1'],capture_output=True)
  main=subprocess.run(['docker','exec',name,'cat','/proc/1/comm'],capture_output=True,text=True)
  if ready.returncode==0 and main.stdout.strip().startswith('clickhouse'):break
  time.sleep(1)
 else:raise RuntimeError('disposable server never became ready')
 for file in sorted(Path('migrations/clickhouse').glob('*.sql')):
  subprocess.run(['docker','exec','-i',name,'clickhouse-client','--config-file','/tmp/check-client.xml','--multiquery','--database','streamforge'],input=file.read_bytes(),check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 for view in ['revenue_1m_mv','traffic_1m_mv','event_identity_values_mv']:sql('ALTER TABLE '+view+' MODIFY SQL SECURITY DEFINER DEFINER=CURRENT_USER')
 digest=hashlib.sha256(password.encode()).hexdigest()
 for user in [query,consumer]:sql(f"CREATE USER {user} IDENTIFIED WITH sha256_hash BY '{digest}'")
 sql(f'GRANT SELECT ON streamforge.revenue_1m_canonical TO {query}');sql(f'GRANT SELECT ON streamforge.traffic_1m_canonical TO {query}')
 sql(f'GRANT SELECT ON system.view_refreshes TO {query}');sql(f'GRANT SHOW TABLES ON streamforge.* TO {query}')
 sql(f'GRANT INSERT ON streamforge.events_raw TO {consumer}');sql(f'GRANT SELECT(event_id) ON streamforge.event_identity_values TO {consumer}')
 sql('SELECT count() FROM revenue_1m_canonical',query);sql('SELECT count() FROM system.view_refreshes',query)
 denied('SELECT count() FROM events_raw',query);denied('SELECT payload_json FROM events_raw LIMIT 1',consumer);denied('CREATE TABLE phase6_denied(x UInt8) ENGINE=Memory',consumer)
 sql('SELECT count() FROM event_identity_values WHERE 0',consumer)
 at=datetime.now(timezone.utc).isoformat();event=dict(schema_version=1,event_id=str(uuid.uuid4()),tenant_id='phase6_acl_'+suffix,user_id='test_user',event_type='page_view',occurred_at=at)
 stamp=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
 row=event|dict(occurred_at=stamp,received_at=stamp,kafka_topic='phase6-grant-check',kafka_partition=0,kafka_offset=0,sink_batch_id=suffix,payload_json=json.dumps(event,separators=(',',':')))
 sql('INSERT INTO events_raw FORMAT JSONEachRow\n'+json.dumps(row),consumer)
 denied('INSERT INTO events_raw SELECT * FROM events_raw WHERE 0',query)
 print('PASS isolated query aggregate/refresh reads; denied raw/insert; consumer insert+MV/readiness; denied DDL')
finally:
 subprocess.run(['docker','stop','--time','10',name],check=True,stdout=subprocess.DEVNULL)
