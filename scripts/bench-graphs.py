"""Render measured benchmark data; never substitute targets for observations."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);p.add_argument('--stem');a=p.parse_args()
s=json.loads((a.directory/'summary.json').read_text());out=Path('load/results');out.mkdir(exist_ok=True)
stem=a.stem or a.directory.name
quality_path=a.directory/'measurement-quality.json'
interrupted=quality_path.exists() and json.loads(quality_path.read_text()).get('host_sleep_detected')
if not re.fullmatch(r'[A-Za-z0-9_-]+',stem):p.error('safe output stem required')
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
  data=series.setdefault(name,[])
  if data and x-data[-1][0]>20/60:data.append((x,float('nan'),float('nan')))
  data.append((x,memory,cpu));peak=peaks.setdefault(name,{'sampled_peak_memory_mib':0,'sampled_peak_cpu_percent':0});peak['sampled_peak_memory_mib']=max(peak['sampled_peak_memory_mib'],memory);peak['sampled_peak_cpu_percent']=max(peak['sampled_peak_cpu_percent'],cpu)
for name,rows in series.items():
 axes[1].plot([r[0] for r in rows],[r[1] for r in rows],label=name);axes[2].plot([r[0] for r in rows],[r[2] for r in rows],label=name)
axes[1].set(xlabel='Minutes from steady start',ylabel='Sampled memory (MiB)');axes[1].legend(ncol=3,fontsize=8)
axes[2].set(xlabel='Minutes from steady start',ylabel='CPU % (100% = one core)')
fig.suptitle('StreamForge 900-second interval — '+('INTERRUPTED BY HOST SLEEP' if interrupted else 'all services share host'))
fig.savefig(out/(stem+'.png'),dpi=160);plt.close(fig)
(out/(stem+'-resources.json')).write_text(json.dumps({'sampling':'approximately 6 seconds; not continuous maxima','samples':len(resources),'containers':peaks},indent=2)+'\n')
print(out/(stem+'.png'))
fig,axes=plt.subplots(2,2,figsize=(10,7),layout='constrained')
axes[0,0].bar(['Client*' if interrupted else 'Client','Server estimate','Query success'],[1000*(s['client_ack_p95_seconds'] or float('nan')),1000*(s['server_ack_p95_histogram_estimate_seconds'] or float('nan')),1000*(s['query_p95_seconds'] or float('nan'))],color=['steelblue','steelblue','seagreen'])
axes[0,0].set(title='Successful latency p95',ylabel='Milliseconds');axes[0,0].tick_params(axis='x',labelsize=8)
if interrupted:axes[0,0].text(.02,.96,'*Interrupted baseline: headers only',transform=axes[0,0].transAxes,va='top',fontsize=8)
axes[0,1].bar(['p50','p95','p99','max'],[s['freshness_success_p50_seconds'] or float('nan'),s['freshness_success_p95_seconds'] or float('nan'),s['freshness_success_p99_seconds'] or float('nan'),s['freshness_success_max_seconds'] or float('nan')],color='steelblue');axes[0,1].axhline(5,color='crimson',linestyle='--');axes[0,1].set(title='Freshness: successful probes only',ylabel='Seconds')
axes[1,0].bar(['Success','Failed/censored','Driver dropped'],[s['freshness_samples']-s['freshness_censored_or_failed'],s['freshness_censored_or_failed'],s['freshness_driver_dropped']],color=['seagreen','crimson','gray']);axes[1,0].set(title='Freshness sample outcomes',ylabel='Probes');axes[1,0].tick_params(axis='x',labelsize=8)
axes[1,1].bar(['Success','Error'],[s['query_count']-s['query_errors'],s['query_errors']],color=['seagreen','crimson']);axes[1,1].set(title='Concurrent query outcomes',ylabel='Requests')
if s['query_p95_seconds'] is None:axes[0,0].text(.02,.85,'Query: no successful responses',transform=axes[0,0].transAxes,fontsize=8,bbox=dict(facecolor='white',alpha=.9,edgecolor='none'))
if s['freshness_success_p95_seconds'] is None:
 axes[0,1].clear();axes[0,1].set_title('Freshness quantiles unavailable');axes[0,1].axis('off')
 axes[0,1].text(.5,.5,'No successful probes\nNo p50 / p95 / p99 / max measurement',transform=axes[0,1].transAxes,ha='center',va='center')
fig.suptitle('Measured outcomes — '+('INTERRUPTED; no continuous SLO claim' if interrupted else stem))
fig.savefig(out/(stem+'-outcomes.png'),dpi=160);plt.close(fig)
