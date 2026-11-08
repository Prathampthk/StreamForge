#!/usr/bin/env python3
"""Create NEW private local credentials/certificates; never upload or rotate AWS secrets."""
import argparse,base64,json,os,re,secrets,subprocess,uuid
from pathlib import Path
from urllib.parse import quote
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--name',default='streamforge-dev');p.add_argument('--out',type=Path,default=Path('.secrets/aws-dev'));a=p.parse_args()
if not re.fullmatch(r'[a-z][a-z0-9-]{2,19}',a.name):p.error('safe lowercase project name required')
if a.out.exists():p.error('refusing to overwrite existing credentials; choose a new directory')
a.out.mkdir(parents=True,mode=0o700);os.chmod(a.out,0o700)
def run(*args):subprocess.run(['openssl',*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
def read(name):return (a.out/name).read_text()
def write(name,value):
 path=a.out/name;path.write_text(value);os.chmod(path,0o600)
run('req','-x509','-newkey','rsa:3072','-nodes','-keyout',str(a.out/'ca.key'),'-out',str(a.out/'ca.pem'),'-days','365','-sha256','-subj','/CN=StreamForge development CA','-addext','basicConstraints=critical,CA:TRUE')
for service in ['kafka','metadata','clickhouse']:
 hostname=f'{service}.{a.name}.internal';write(service+'.ext',f'subjectAltName=DNS:{hostname},IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n')
 run('req','-new','-newkey','rsa:3072','-nodes','-keyout',str(a.out/(service+'.key')),'-out',str(a.out/(service+'.csr')),'-subj','/CN='+hostname)
 run('x509','-req','-in',str(a.out/(service+'.csr')),'-CA',str(a.out/'ca.pem'),'-CAkey',str(a.out/'ca.key'),'-CAcreateserial','-out',str(a.out/(service+'.pem')),'-days','30','-sha256','-extfile',str(a.out/(service+'.ext')))
for f in a.out.iterdir():os.chmod(f,0o600)
kafka={k:secrets.token_urlsafe(32) for k in ['admin','ingest','consumer','replay']};pg={k:secrets.token_urlsafe(32) for k in ['admin','consumer','replay']};ch={k:secrets.token_urlsafe(32) for k in ['admin','consumer','query']};ca=read('ca.pem')
platform={'ca_pem':ca,'kafka_cert_pem':read('kafka.pem'),'kafka_key_pem':read('kafka.key'),'postgres_cert_pem':read('metadata.pem'),'postgres_key_pem':read('metadata.key'),'kafka_users':kafka,'postgres_passwords':pg,'kafka_cluster_id':base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')}
clickhouse={'ca_pem':ca,'cert_pem':read('clickhouse.pem'),'key_pem':read('clickhouse.key'),'passwords':ch}
def broker(user):return {'KAFKA_CA_PEM':ca,'KAFKA_SASL_USERNAME':user,'KAFKA_SASL_PASSWORD':kafka[user]}
def postgres(user):return {'POSTGRES_CA_PEM':ca,'POSTGRES_DSN':f'postgres://sf_{user}:{quote(pg[user],safe="")}@metadata.{a.name}.internal:5432/streamforge?sslmode=verify-full'}
values={'platform':platform,'clickhouse':clickhouse,'ingest':broker('ingest')|{'INGEST_API_KEY':secrets.token_urlsafe(32)},'consumer':broker('consumer')|postgres('consumer')|{'CLICKHOUSE_CA_PEM':ca,'CLICKHOUSE_PASSWORD':ch['consumer']},'query':{'QUERY_API_KEY':secrets.token_urlsafe(32),'CLICKHOUSE_CA_PEM':ca,'CLICKHOUSE_PASSWORD':ch['query']},'replay':broker('replay')|postgres('replay')}
for name,value in values.items():write(name+'.json',json.dumps(value,indent=2)+'\n')
print('Created private local material. No AWS operation performed. Leaf certificates expire in 30 days; retain the CA key offline.')
