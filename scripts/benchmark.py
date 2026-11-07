"""Run the versioned bounded local benchmark profile; never provision cloud resources."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import uuid
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--rate',type=int,default=10500);p.add_argument('--duration',default='15m');p.add_argument('--name',default='steady-15m');a=p.parse_args()
if not a.name.replace('-','').replace('_','').isalnum():p.error('safe result name required')
match=re.fullmatch(r'(\d+)([sm])',a.duration)
if not match:p.error('duration must use integer seconds or minutes')
seconds=int(match[1])*(60 if match[2]=='m' else 1)
if not 1<=seconds<=1800 or not 1<=a.rate<=100000 or seconds*a.rate>11000000:p.error('duration/rate exceeds bounded profile')
tenant='bench_'+uuid.uuid4().hex[:12]
env=os.environ|{'BENCH_TENANT':tenant,'LOCAL_UID':str(os.getuid()),'LOCAL_GID':str(os.getgid())}
compose=['docker','compose','--env-file','.env','-f','deploy/compose.yaml','-f','deploy/compose.bench.yaml']
subprocess.run(compose+['up','-d','--build','--wait','--wait-timeout','180','bench-ingest','bench-query'],env=env,check=True)
directory=Path('load/results/raw')/a.name
if directory.exists():p.error('result directory already exists; choose a new name')
directory.mkdir(parents=True)
spec=dict(recorded_at=datetime.now(timezone.utc).isoformat(),platform=platform.platform(),tenant=tenant,offered_rate=a.rate,duration=a.duration,query_concurrency=2,query_windows=['15m','24h'],history_seed_events=1440,all_components_share_host=True,consumer_replicas=1,consumer_batch_records=1000,consumer_workers=4,kafka_partitions=12,kafka_replication_factor=1,compression='lz4',acks='all',producer_idempotence=True,trace_root_sample_ratio_for_benchmark_endpoints=0)
for key,command in {'host_cpu':['sysctl','-n','machdep.cpu.brand_string'],'host_cores':['sysctl','-n','hw.ncpu'],'host_memory_bytes':['sysctl','-n','hw.memsize'],'docker':['docker','info','--format','cpus={{.NCPU}} memory={{.MemTotal}}'],'disk_free':['df','-h','.']}.items():spec[key]=subprocess.check_output(command,text=True).strip()
(directory/'environment.json').write_text(json.dumps(spec,indent=2)+'\n')
monitor=subprocess.Popen(['python3','scripts/bench-monitor.py',str(directory),'--seconds',str(seconds+90)])
try:
 result=subprocess.run(compose+['run','--rm','--no-deps','eventgen','--duration',a.duration,'--rate',str(a.rate),'--batch','100','--workers','32','--out','/results/'+a.name,'--tenant',tenant],env=env)
finally:
 monitor.terminate()
 try:monitor.wait(timeout=15)
 except subprocess.TimeoutExpired:monitor.kill();monitor.wait()
raise SystemExit(result.returncode)
