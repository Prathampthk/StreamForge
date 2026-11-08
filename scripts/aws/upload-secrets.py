#!/usr/bin/env python3
"""Explicitly authorized upload to EXISTING reviewed secret ARNs, not secret creation."""
import argparse,json,re,subprocess
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--confirm',required=True);p.add_argument('--directory',type=Path,required=True);p.add_argument('--outputs',type=Path,required=True);a=p.parse_args()
if a.confirm!='Upload reviewed secrets':p.error('explicit confirmation required')
outputs=json.loads(a.outputs.read_text());arns=outputs['secret_arns']['value'];account=subprocess.check_output(['aws','sts','get-caller-identity','--query','Account','--output','text'],text=True).strip()
for name in ['platform','clickhouse','ingest','consumer','query','replay']:
 arn=arns[name]
 if not re.fullmatch(r'arn:aws:secretsmanager:[a-z0-9-]+:'+account+r':secret:[A-Za-z0-9/_-]+',arn):raise RuntimeError('secret ARN/account mismatch')
 path=(a.directory/(name+'.json')).resolve()
 if path.stat().st_mode&0o077:raise RuntimeError('private secret file permissions required')
 # Never print the JSON document or pass secret values as process arguments.
 subprocess.run(['aws','secretsmanager','put-secret-value','--secret-id',arn,'--secret-string','file://'+str(path)],check=True,stdout=subprocess.DEVNULL)
print('Reviewed existing secrets populated; no values logged.')
