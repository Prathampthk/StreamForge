"""Low-rate resource/refresh sampling for one bounded local benchmark."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
import signal
from reconcile import ClickHouse
p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);p.add_argument('--seconds',type=int,default=950);a=p.parse_args()
if not 1<=a.seconds<=1900:p.error('seconds out of bounds')
a.directory.mkdir(parents=True,exist_ok=True);client=ClickHouse();deadline=time.monotonic()+a.seconds
stopping=False
def stop(signum,frame):
 global stopping
 stopping=True
signal.signal(signal.SIGTERM,stop)
with (a.directory/'resources.jsonl').open('w') as stream:
 while not stopping and time.monotonic()<deadline:
  sample=dict(at=datetime.now(timezone.utc).isoformat())
  result=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}','streamforge-clickhouse-1','streamforge-kafka-1','streamforge-consumer-1','streamforge-bench-ingest-1','streamforge-bench-query-1'],text=True,capture_output=True,timeout=10)
  sample['containers']=[json.loads(row) for row in result.stdout.splitlines() if row.startswith('{')]
  try:sample['refresh']=client.query("SELECT view,status,toUnixTimestamp(last_success_time) AS last_success,toUInt8(exception!='') AS failed FROM system.view_refreshes WHERE database='streamforge'",{})
  except Exception as error:sample['refresh_error']=type(error).__name__
  stream.write(json.dumps(sample)+'\n');stream.flush();time.sleep(min(5,max(0,deadline-time.monotonic())))
