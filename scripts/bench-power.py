"""Record macOS sleep/wake evidence and disqualify an interrupted measurement."""
import argparse
from datetime import datetime, timezone, timedelta
import json
import platform
import subprocess
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
spec=json.loads((a.directory/'environment.json').read_text());start=datetime.fromisoformat(spec['recorded_at']);end=datetime.now(timezone.utc)
checked_at=end
summary_path=a.directory/'summary.json'
if summary_path.exists():
 summary=json.loads(summary_path.read_text());start=datetime.fromisoformat(summary['started_at'].replace('Z','+00:00'));end=start+timedelta(seconds=summary['duration_seconds'])
result={'checked_at':checked_at.isoformat(),'host_sleep_detected':None,'uninterrupted_qualification':'unknown','scope':'host power events inside steady interval when summary exists; absence alone is not a throughput guarantee'}
if platform.system()=='Darwin':
 try:
  raw=subprocess.check_output(['pmset','-g','log'],text=True,timeout=20)
  events=[]
  for line in raw.splitlines():
   try:at=datetime.strptime(line[:25],'%Y-%m-%d %H:%M:%S %z')
   except ValueError:continue
   if start<=at<=end and ('Entering Sleep state' in line or 'Wake from ' in line):events.append(line)
  (a.directory/'host-power-events.log').write_text('\n'.join(events)+'\n')
  result['host_sleep_detected']=any('Entering Sleep state' in line for line in events)
  result['uninterrupted_qualification']='interrupted' if result['host_sleep_detected'] else 'no_sleep_recorded'
 except Exception as error:result['power_log_error']=type(error).__name__
(a.directory/'measurement-quality.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
