#!/usr/bin/env python3
"""Offline certificate and stateful-config checks. No AWS calls or daemon operations."""
import json,subprocess,tempfile
from pathlib import Path
from xml.etree import ElementTree as ET
with tempfile.TemporaryDirectory(prefix='streamforge-private-') as directory:
 root=Path(directory);material=root/'material'
 subprocess.run(['python3','scripts/aws/prepare-secrets.py','--out',str(material)],check=True)
 for name in ['kafka','metadata','clickhouse']:
  subprocess.run(['openssl','verify','-CAfile',str(material/'ca.pem'),'-verify_hostname',name+'.streamforge-dev.internal',str(material/(name+'.pem'))],check=True)
 wrong=subprocess.run(['openssl','verify','-CAfile',str(material/'ca.pem'),'-verify_hostname','wrong.internal',str(material/'kafka.pem')],capture_output=True)
 assert wrong.returncode!=0
 images={k:'123456789012.dkr.ecr.us-east-2.amazonaws.com/streamforge-dev/'+k+'@sha256:'+'a'*64 for k in ['kafka','postgres','clickhouse']}
 for role in ['kafka','clickhouse']:
  out=root/role
  subprocess.run(['python3','scripts/aws/stateful.py','--role',role,'--volume','vol-0123456789abcdef0','--region','us-east-2','--bucket','offline-test','--release','a'*40,'--secret','offline','--images',json.dumps(images),'--domain','streamforge-dev.internal','--log-group','offline','--render-only','--secret-file',str(material/('platform.json' if role=='kafka' else 'clickhouse.json')),'--directory',str(out)],check=True)
 props=(root/'kafka/server.properties').read_text()
 assert 'CLIENT:SASL_SSL' in props and 'User:ANONYMOUS' not in props and 'allow.everyone.if.no.acl.found=false' in props
 assert 'hostssl streamforge sf_consumer' in (root/'kafka/pg_hba.conf').read_text()
 config=ET.parse(root/'clickhouse/secure.xml').getroot()
 assert config.find('https_port').text=='8443' and config.find('http_port').get('remove')=='1'
 for path in material.iterdir():assert path.stat().st_mode&0o077==0
print('PASS offline leaf identity, wrong-host rejection, private file modes and secure rendered listeners')
