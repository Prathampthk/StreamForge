"""Render measured benchmark data; never substitute targets for observations."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args()
s=json.loads((a.directory/'summary.json').read_text());out=Path('load/results');out.mkdir(exist_ok=True)
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
fig,axes=plt.subplots(3,1,figsize=(10,9),layout='constrained')
axes[0].plot([r['second']/60 for r in s['per_second']],[r['acknowledged_events'] for r in s['per_second']],linewidth=.8,label='Kafka acknowledged')
axes[0].axhline(10000,color='crimson',linestyle='--',label='Target 10,000/s');axes[0].set(xlabel='Steady interval (minutes)',ylabel='Acknowledged events/s');axes[0].legend()
resources=[json.loads(line) for line in (a.directory/'resources.jsonl').read_text().splitlines()]
t0=datetime.fromisoformat(s['started_at'].replace('Z','+00:00'))
series={};peaks={}
def mib(value):
 match=re.match(r'([\d.]+)([A-Za-z]+)',value)
 if not match:return 0
 return float(match[1])*{'B':1/1048576,'kB':1/1024,'KiB':1/1024,'MB':1,'MiB':1,'GB':1024,'GiB':1024}.get(match[2],1)
for r in resources:
 x=(datetime.fromisoformat(r['at'])-t0).total_seconds()/60
 for c in r.get('containers',[]):
  name=c['Name'].removeprefix('streamforge-').removesuffix('-1');memory=mib(c['MemUsage'].split(' / ')[0]);cpu=float(c['CPUPerc'].rstrip('%'))
  series.setdefault(name,[]).append((x,memory,cpu));peak=peaks.setdefault(name,{'sampled_peak_memory_mib':0,'sampled_peak_cpu_percent':0});peak['sampled_peak_memory_mib']=max(peak['sampled_peak_memory_mib'],memory);peak['sampled_peak_cpu_percent']=max(peak['sampled_peak_cpu_percent'],cpu)
for name,rows in series.items():
 axes[1].plot([r[0] for r in rows],[r[1] for r in rows],label=name);axes[2].plot([r[0] for r in rows],[r[2] for r in rows],label=name)
axes[1].set(xlabel='Minutes from steady start',ylabel='Sampled memory (MiB)');axes[1].legend(ncol=3,fontsize=8)
axes[2].set(xlabel='Minutes from steady start',ylabel='CPU % (100% = one core)')
fig.suptitle('StreamForge local 15-minute measurement — all services share host')
fig.savefig(out/'steady-15m.png',dpi=160);plt.close(fig)
(out/'resources-summary.json').write_text(json.dumps({'sampling':'approximately 6 seconds; not continuous maxima','samples':len(resources),'containers':peaks},indent=2)+'\n')
print(out/'steady-15m.png')
