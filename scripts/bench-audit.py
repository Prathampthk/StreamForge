"""Stream large accepted receipts; compare every canonical ID/hash and exact business sums."""
import argparse
from collections import defaultdict
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import time
from reconcile import ClickHouse, instant

parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('directory',type=Path);parser.add_argument('--drain-timeout',type=int,default=120);args=parser.parse_args()
if not 0<=args.drain_timeout<=600:parser.error('drain timeout must be <=600')
client=ClickHouse();summary=json.loads((args.directory/'summary.json').read_text());tenant=summary['tenant'];expected_revenue=defaultdict(lambda:[0,0]);expected_traffic=defaultdict(lambda:[0,0,0])
counts=dict(accepted=0,missing=0,payload_mismatch=0,unverified_read_error=0);ids=[];hashes={};examples=[]
global_deadline=time.monotonic()+args.drain_timeout
def add_example(value):
 if len(examples)<20:examples.append(value)
def checked_verify(chunk,expected):
 try:verify(chunk,expected)
 except Exception as error:
  counts['unverified_read_error']+=len(chunk)
  for value in chunk[:20]:add_example(value)
  print(json.dumps(dict(audit_read_error=type(error).__name__,unverified_events=len(chunk))),flush=True)
def verify(chunk,expected):
 params=dict(ids='['+','.join("'"+value+"'" for value in chunk)+']')
 observed={row['event_id']:row['checksum'].lower() for row in client.query('SELECT event_id,hex(SHA256(payload)) AS checksum FROM events_canonical WHERE event_id IN {ids:Array(UUID)} LIMIT 1025',params)}
 deadline=global_deadline
 while len(observed)<len(chunk) and time.monotonic()<deadline:
  time.sleep(.5);observed={row['event_id']:row['checksum'].lower() for row in client.query('SELECT event_id,hex(SHA256(payload)) AS checksum FROM events_canonical WHERE event_id IN {ids:Array(UUID)} LIMIT 1025',params)}
 for value in chunk:
  if value not in observed:counts['missing']+=1;add_example(value)
  elif observed[value]!=expected[value]:counts['payload_mismatch']+=1;add_example(value)
with (args.directory/'accepted.jsonl').open() as stream:
 for line in stream:
  receipt=json.loads(line);event=receipt['event'];value=event['event_id'];counts['accepted']+=1;ids.append(value);hashes[value]=receipt['payload_sha256']
  minute=instant(event['occurred_at']).replace(second=0,microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ');traffic=expected_traffic[minute];traffic[0]+=1
  if event['event_type']=='api_request':traffic[1]+=1;traffic[2]+=int(event['status_code']>=500)
  if event['event_type']=='purchase':revenue=expected_revenue[(minute,event['currency'])];revenue[0]+=1;revenue[1]+=event['amount_cents']
  if len(ids)==1000:checked_verify(ids,hashes);ids=[];hashes={}
 if ids:checked_verify(ids,hashes)
params=dict(tenant=tenant)
def metrics():
 revenue={(r['minute'],r['currency']):[int(r['purchases']),int(r['cents'])] for r in client.query("SELECT formatDateTime(snapshot.minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,currency,sum(purchase_count) AS purchases,toString(sum(revenue_cents)) AS cents FROM revenue_1m_canonical AS snapshot WHERE tenant_id={tenant:String} GROUP BY snapshot.minute,currency",params)}
 traffic={r['minute']:[int(r['events']),int(r['rpm']),int(r['errors'])] for r in client.query("SELECT formatDateTime(snapshot.minute,'%Y-%m-%dT%H:%i:%SZ','UTC') AS minute,sum(events_per_minute) AS events,sum(rpm) AS rpm,sum(api_errors) AS errors FROM traffic_1m_canonical AS snapshot WHERE tenant_id={tenant:String} GROUP BY snapshot.minute",params)}
 return revenue,traffic
# A tenant is unique per formal run. Unknown delivery outcomes can create extra
# data and must be reported, never labeled loss or removed to improve results.
metrics_error=None
try:
 revenue,traffic=metrics();until=time.monotonic()+args.drain_timeout
 while (revenue!=dict(expected_revenue) or traffic!=dict(expected_traffic)) and time.monotonic()<until:
  time.sleep(1);revenue,traffic=metrics()
except Exception as error:
 revenue,traffic=None,None;metrics_error=type(error).__name__
result=dict(counts,canonical_ids_verified=(counts['missing']==0 and counts['payload_mismatch']==0 and counts['unverified_read_error']==0),revenue_exact=(revenue==dict(expected_revenue)),traffic_exact=(traffic==dict(expected_traffic)),unknown_delivery_events=summary['unknown_events_all_stages'],examples=examples[:20],drain_limit_seconds=args.drain_timeout)
result['metrics_read_error']=metrics_error
result['ok']=result['canonical_ids_verified'] and result['revenue_exact'] and result['traffic_exact'] and not result['unknown_delivery_events']
(args.directory/'audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));raise SystemExit(0 if result['ok'] else 1)
