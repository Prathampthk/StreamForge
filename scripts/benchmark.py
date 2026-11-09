"""Run the versioned bounded local benchmark profile; never provision cloud resources."""
import argparse
from datetime import datetime, timezone
import json
import hashlib
from reconcile import ClickHouse
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import uuid
import time
from urllib.request import urlopen
from urllib.error import HTTPError
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--rate',type=int,default=10500);p.add_argument('--duration',default='15m');p.add_argument('--name',default='steady-15m');p.add_argument('--allow-unready-query',action='store_true',help='measure degraded analytics explicitly; retain all query/probe failures');a=p.parse_args()
if not a.name.replace('-','').replace('_','').isalnum():p.error('safe result name required')
match=re.fullmatch(r'(\d+)([sm])',a.duration)
if not match:p.error('duration must use integer seconds or minutes')
seconds=int(match[1])*(60 if match[2]=='m' else 1)
if not 1<=seconds<=1800 or not 1<=a.rate<=100000 or seconds*a.rate>11000000:p.error('duration/rate exceeds bounded profile')
tenant='bench_'+uuid.uuid4().hex[:12]
env=os.environ|{'BENCH_TENANT':tenant,'LOCAL_UID':str(os.getuid()),'LOCAL_GID':str(os.getgid())}
compose=['docker','compose','--env-file','.env','-f','deploy/compose.yaml','-f','deploy/compose.bench.yaml']
startup=['up','-d','--build']
if not a.allow_unready_query:startup+=['--wait','--wait-timeout','180']
subprocess.run(compose+startup+['bench-ingest','bench-query'],env=env,check=True)
startup_deadline=time.monotonic()+60
while True:
 try:
  with urlopen('http://127.0.0.1:8083/readyz',timeout=2) as response:assert response.status==200
  break
 except Exception:
  if time.monotonic()>=startup_deadline:raise RuntimeError('ingestion never became ready')
  time.sleep(1)
try:
 with urlopen('http://127.0.0.1:8084/readyz',timeout=3) as response:query_baseline_status=response.status
except HTTPError as error:query_baseline_status=error.code
except Exception:query_baseline_status=0
directory=Path('load/results/raw')/a.name
if directory.exists():p.error('result directory already exists; choose a new name')
directory.mkdir(parents=True)
spec=dict(recorded_at=datetime.now(timezone.utc).isoformat(),platform=platform.platform(),tenant=tenant,offered_rate=a.rate,duration=a.duration,query_concurrency=2,query_windows=['15m','24h'],query_endpoints=['traffic','revenue'],query_revenue_currencies=['USD','EUR'],query_schedule='per-window caller alternates traffic and revenue; USD/EUR alternate',history_seed_events=1440,all_components_share_host=True,consumer_replicas=1,consumer_batch_records=1000,consumer_workers=4,kafka_partitions=12,kafka_replication_factor=1,compression='lz4',acks='all',producer_idempotence=True,trace_root_sample_ratio_for_benchmark_endpoints=0)
for key,command in {'host_cpu':['sysctl','-n','machdep.cpu.brand_string'],'host_cores':['sysctl','-n','hw.ncpu'],'host_memory_bytes':['sysctl','-n','hw.memsize'],'docker':['docker','info','--format','cpus={{.NCPU}} memory={{.MemTotal}}'],'disk_free':['df','-h','.']}.items():spec[key]=subprocess.check_output(command,text=True).strip()
spec['allow_unready_query']=a.allow_unready_query
spec['initial_query_ready_http_status']=query_baseline_status
spec['initial_refresh_health']=ClickHouse().query("SELECT view,status,last_success_time,substring(exception,1,200) AS exception FROM system.view_refreshes WHERE database='streamforge'",{})
spec['source_revision']=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
spec['clickhouse_migration_sha256']={str(file):hashlib.sha256(file.read_bytes()).hexdigest() for file in sorted(Path('migrations/clickhouse').glob('*.sql'))}
spec['clickhouse_resource_profile_sha256']=hashlib.sha256(Path('deploy/clickhouse/resources.xml').read_bytes()).hexdigest()
spec['clickhouse_identity_and_log_engines']=ClickHouse().query("SELECT database,name,create_table_query FROM system.tables WHERE (database='streamforge' AND name IN ('event_identity_states','event_identity_values')) OR (database='system' AND name IN ('metric_log','query_metric_log'))",{})
digest=hashlib.sha256()
for source in sorted([*Path('cmd').rglob('*.go'),*Path('internal').rglob('*.go')]):
 if not source.name.endswith('_test.go'):digest.update(str(source).encode()+b'\0'+source.read_bytes())
spec['go_source_sha256']=digest.hexdigest()
spec['initial_clickhouse_parts']=ClickHouse().query("SELECT table,sum(rows) AS physical_rows,sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE database='streamforge' AND active GROUP BY table",{})
spec['runtime_image_ids']=subprocess.check_output(['docker','inspect','--format','{{.Name}} {{.Image}}','streamforge-bench-ingest-1','streamforge-bench-query-1','streamforge-consumer-1','streamforge-clickhouse-1','streamforge-kafka-1'],text=True).strip()
(directory/'environment.json').write_text(json.dumps(spec,indent=2)+'\n')
# Scoped inhibitor prevents idle sleep; it cannot override closing the lid.
inhibitor=subprocess.Popen(['/usr/bin/caffeinate','-i','-w',str(os.getpid())]) if platform.system()=='Darwin' and shutil.which('caffeinate') else None
monitor=subprocess.Popen(['python3','scripts/bench-monitor.py',str(directory),'--seconds',str(seconds+90)])
try:
 result=subprocess.run(compose+['run','--rm','--no-deps','eventgen','--duration',a.duration,'--rate',str(a.rate),'--batch','100','--workers','32','--out','/results/'+a.name,'--tenant',tenant,'--isolated-tenant'],env=env)
finally:
 monitor.terminate()
 try:monitor.wait(timeout=15)
 except subprocess.TimeoutExpired:monitor.kill();monitor.wait()
 if inhibitor is not None:inhibitor.terminate();inhibitor.wait(timeout=5)
 subprocess.run(['python3','scripts/bench-power.py',str(directory)],check=True)
raise SystemExit(result.returncode)
