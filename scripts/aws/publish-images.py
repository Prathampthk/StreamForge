#!/usr/bin/env python3
"""Explicitly authorized image publication only. No Terraform/ECS/EC2 write access needed."""
import argparse,json,os,re,subprocess
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--confirm',required=True);a=p.parse_args()
if a.confirm!='Publish reviewed images':p.error('explicit confirmation required')
def run(args,**kw):return subprocess.run(args,check=True,text=True,capture_output=True,**kw).stdout.strip()
account=os.environ['AWS_ACCOUNT_ID'];region=os.environ['AWS_REGION'];prefix=os.environ['ECR_PREFIX']
if not re.fullmatch(r'\d{12}',account) or not re.fullmatch(r'[a-z0-9-]+',region) or not re.fullmatch(r'[a-z][a-z0-9-]{2,23}',prefix):p.error('invalid account/region/project')
if run(['aws','sts','get-caller-identity','--query','Account','--output','text'])!=account:raise RuntimeError('wrong AWS account')
revision=run(['git','rev-parse','HEAD']);registry=f'{account}.dkr.ecr.{region}.amazonaws.com';token=run(['aws','ecr','get-login-password','--region',region]);run(['docker','login','--username','AWS','--password-stdin',registry],input=token)
images={}
try:
 for name in ['ingest-api','consumer','query-api','tools']:
  dockerfile={'ingest-api':'ingest','consumer':'consumer','query-api':'query','tools':'tools'}[name]
  run(['docker','buildx','build','--load','--provenance=false','--output','type=docker,rewrite-timestamp=true','--build-arg','SOURCE_DATE_EPOCH='+run(['git','show','-s','--format=%ct','HEAD']),'--platform','linux/amd64','-f',f'deploy/docker/{dockerfile}.Dockerfile','-t',f'{registry}/{prefix}/{name}:{revision}', '.'])
 for name,source in json.loads(Path('deploy/vendor-images.json').read_text()).items():
  run(['docker','pull','--platform','linux/amd64',source]);run(['docker','tag',source,f'{registry}/{prefix}/{name}:{revision}'])
 for name in ['ingest-api','consumer','query-api','tools','kafka','clickhouse','postgres']:
  image=f'{registry}/{prefix}/{name}:{revision}'
  subprocess.run(['docker','run','--rm','-v','/var/run/docker.sock:/var/run/docker.sock','aquasec/trivy:0.76.2@sha256:8f6bb947dbc528902e11f2f5be52f7b519f6a39d5d148f41166f2040499768fb','image','--scanners','vuln,secret','--severity','HIGH,CRITICAL','--exit-code','1',image],check=True)
 # Every image must pass BEFORE the first publication.
 for name in ['ingest-api','consumer','query-api','tools','kafka','clickhouse','postgres']:
  image=f'{registry}/{prefix}/{name}:{revision}';run(['docker','push','--platform','linux/amd64',image])
  digest=run(['aws','ecr','describe-images','--region',region,'--repository-name',prefix+'/'+name,'--image-ids','imageTag='+revision,'--query','imageDetails[0].imageDigest','--output','text'])
  if not re.fullmatch('sha256:[a-f0-9]{64}',digest):raise RuntimeError('invalid published digest')
  images[name]=f'{registry}/{prefix}/{name}@{digest}'
 Path('.aws-artifacts').mkdir(exist_ok=True);Path('.aws-artifacts/images.json').write_text(json.dumps(images,indent=2)+'\n')
finally:run(['docker','logout',registry])
