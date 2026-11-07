"""Reversible faults against this project's local containers; never delete volumes."""
import json
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid
from reconcile import ClickHouse

compose=['docker','compose','--env-file','.env','-f','deploy/compose.yaml']
client=ClickHouse()
results=[]
def command(*args):subprocess.run(compose+list(args),check=True,stdout=subprocess.DEVNULL,timeout=180)
def post(event):
 request=Request('http://localhost:8080/v1/events',data=json.dumps(event).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer streamforge_demo_key_local_only_change_me'})
 try:
  with urlopen(request,timeout=10) as response:return response.status,json.loads(response.read())
 except HTTPError as error:
  with error:return error.code,json.loads(error.read())
def event():return dict(schema_version=1,event_id=str(uuid.uuid4()),tenant_id='merchant_demo',user_id='fault_operator',event_type='purchase',occurred_at=datetime.now(timezone.utc).isoformat(),order_id='fault_order',amount_cents=100,currency='USD')
def wait_ids(rows):
 ids="["+','.join("'"+r['event_id']+"'" for r in rows)+"]"
 for _ in range(120):
  try:
   found=client.query('SELECT count() AS n,toString(sum(toInt128(amount_cents))) AS cents FROM events_canonical WHERE event_id IN {ids:Array(UUID)} AND currency={currency:String}',dict(ids=ids,currency='USD'))[0]
   if int(found['n'])==len(rows) and int(found['cents'])==100*len(rows):return
  except OSError:pass
  time.sleep(.5)
 raise AssertionError('accepted-ID canonical drain failed')
# Start only from a healthy owner; do not confuse deployment setup with a fault.
for _ in range(60):
 state=subprocess.check_output(['docker','inspect','streamforge-consumer-1','--format','{{.State.Health.Status}}'],text=True).strip()
 if state=='healthy':break
 time.sleep(1)
else:raise AssertionError('normal consumer not healthy before fault')
# ClickHouse unavailable must not turn valid data into poison.
accepted=[]
try:
 command('stop','clickhouse')
 for _ in range(5):
  row=event();status,_=post(row);assert status==202,status;accepted.append(row)
 results.append(dict(fault='clickhouse_unavailable',http_202=len(accepted),source='Kafka backlog'))
finally:command('up','-d','--wait','--wait-timeout','120','clickhouse')
wait_ids(accepted)
# Broker unavailable must not produce false HTTP acceptance.
try:
 command('stop','kafka')
 status,body=post(event());assert status==503,(status,body)
 results.append(dict(fault='kafka_unavailable',http_status=status,code=body['code'],delivery='unknown, not claimed absent'))
finally:command('up','-d','--wait','--wait-timeout','180','kafka')
wait_ids(accepted)
for row in results:row['accepted_ids_reconciled']=len(accepted)
Path('load/results/local-faults.json').write_text(json.dumps(dict(executed_at=datetime.now(timezone.utc).isoformat(),results=results),indent=2)+'\n')
print('PASS local faults: ClickHouse outage retained 5 Kafka-acknowledged IDs; broker outage returned 503; prior accepted IDs/revenue reconciled after recovery')
