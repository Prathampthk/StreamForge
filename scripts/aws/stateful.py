#!/usr/bin/env python3
"""Fail-closed, first-boot private EC2 initialization. Never print secret values."""
import argparse,base64,hashlib,json,os,re,subprocess,time
from pathlib import Path
from xml.etree import ElementTree as ET
p=argparse.ArgumentParser(description=__doc__)
for key in ['role','volume','region','bucket','release','secret','images','domain','log-group']:p.add_argument('--'+key,required=True)
p.add_argument('--render-only',action='store_true');p.add_argument('--secret-file',type=Path);p.add_argument('--directory',type=Path,default=Path('/opt/streamforge'));a=p.parse_args()
if a.role not in ['kafka','clickhouse']:p.error('unknown node role')
images=json.loads(a.images)
class CommandFailure(RuntimeError):pass
def run(args,**kw):
 result=subprocess.run(args,text=True,capture_output=True,**kw)
 if result.returncode!=0:raise CommandFailure('bootstrap command failed: '+Path(args[0]).name+' (details withheld)')
 return result.stdout
secret=json.loads(a.secret_file.read_text() if a.secret_file else run(['aws','secretsmanager','get-secret-value','--region',a.region,'--secret-id',a.secret,'--query','SecretString','--output','text']))
root=a.directory;root.mkdir(parents=True,exist_ok=True,mode=0o700)
if not a.render_only:os.chmod(root,0o711)
def write(name,value,uid=None):
 path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(value);os.chmod(path,0o600)
 if uid is not None and not a.render_only:os.chown(path,uid,uid)
 return str(path)
def mount_data():
 deadline=time.monotonic()+600;device=None
 while time.monotonic()<deadline:
  devices=json.loads(run(['lsblk','-J','-o','PATH,SERIAL']))['blockdevices']
  device=next((r['path'] for r in devices if (r.get('serial') or '').replace('-','')==a.volume.replace('-','')),None)
  if device:break
  time.sleep(2)
 if not device:raise RuntimeError('expected EBS volume never attached')
 source=run(['findmnt','-n','-o','SOURCE','/']).strip()
 if source.startswith(device):raise RuntimeError('refusing root device')
 probe=subprocess.run(['blkid','-o','value','-s','TYPE',device],text=True,capture_output=True)
 if probe.returncode==2:run(['mkfs.xfs',device])
 elif probe.returncode!=0 or probe.stdout.strip()!='xfs':raise RuntimeError('refusing existing unknown filesystem')
 uuid=run(['blkid','-o','value','-s','UUID',device]).strip();Path('/data').mkdir(exist_ok=True)
 mounted=subprocess.run(['findmnt','-n','-o','UUID','/data'],capture_output=True,text=True)
 if mounted.returncode==0 and mounted.stdout.strip()!=uuid:raise RuntimeError('unexpected /data mount')
 if mounted.returncode!=0:run(['mount','--uuid',uuid,'/data'])
 fstab=Path('/etc/fstab');line=f'UUID={uuid} /data xfs defaults,nofail 0 2\n'
 if line not in fstab.read_text():
  with fstab.open('a') as stream:stream.write(line)
 marker=Path('/data/.streamforge-owner');owner=a.domain+':'+a.role
 if marker.exists() and marker.read_text()!=owner:raise RuntimeError('data volume owner mismatch')
 if not marker.exists():
  if any(Path('/data').iterdir()):raise RuntimeError('refusing nonempty unowned volume')
  marker.write_text(owner)
def docker(args,**kw):return run(['docker',*args],**kw)
def start(name,image,options,command=()):
 existing=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if existing.returncode==0:
  if json.loads(existing.stdout)[0]['Config']['Image']!=image:raise RuntimeError('existing image differs; review an explicit retained-data redeploy')
  docker(['start',name]);return
 docker(['run','-d','--name',name,'--restart','unless-stopped','--network','host','--log-driver','awslogs','--log-opt','awslogs-region='+a.region,'--log-opt','awslogs-group='+a.log_group,'--log-opt','awslogs-stream='+name,*options,image,*command])
def steady(name,prefix):
 return docker(['exec',name,'cat','/proc/1/comm']).strip().startswith(prefix)
def fetch_schema():
 # Known numbered files are derived from a versioned public (nonsecret) manifest.
 manifest=json.loads(run(['aws','s3','cp',f's3://{a.bucket}/{a.release}/bootstrap/manifest.json','-','--region',a.region]))
 result={}
 for key in manifest:
  if key.startswith('migrations/'+('postgres' if a.role=='kafka' else 'clickhouse')+'/') and key.endswith('.sql'):
   result[key]=run(['aws','s3','cp',f's3://{a.bucket}/{a.release}/{key}','-','--region',a.region])
 return result
def initial_schema(kind,files,apply):
 selected={k:v for k,v in files.items() if k.startswith('migrations/'+kind+'/') and (kind!='postgres' or k.endswith('.up.sql'))}
 digest=hashlib.sha256(json.dumps(selected,sort_keys=True).encode()).hexdigest();marker=Path('/data/.schema-'+kind)
 if marker.exists():
  if marker.read_text()!=digest:raise RuntimeError('schema changed: explicit reviewed migration required')
  return
 for key in sorted(selected):apply(selected[key])
 marker.write_text(digest)
if not a.render_only:
 mount_data();registry=next(iter(images.values())).split('/')[0]
 token=run(['aws','ecr','get-login-password','--region',a.region]);docker(['login','--username','AWS','--password-stdin',registry],input=token)
 try:
  for name in (['kafka','postgres'] if a.role=='kafka' else ['clickhouse']):docker(['pull',images[name]])
 finally:docker(['logout',registry])
 schema=fetch_schema()
if a.role=='kafka':
 if set(secret['kafka_users'])!={'admin','ingest','consumer','replay'} or not all(re.fullmatch(r'[A-Za-z0-9_-]{32,128}',v) for v in secret['kafka_users'].values()):raise RuntimeError('unexpected Kafka credential shape')
 ca=write('ca.pem',secret['ca_pem'],1000)
 write('kafka-keystore.pem',secret['kafka_key_pem']+'\n'+secret['kafka_cert_pem'],1000)
 admin=secret['kafka_users']['admin'];host='kafka.'+a.domain
 props=f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=CLIENT://0.0.0.0:9094,INTERNAL://127.0.0.1:19092,CONTROLLER://127.0.0.1:9093
advertised.listeners=CLIENT://{host}:9094,INTERNAL://127.0.0.1:19092
listener.security.protocol.map=CLIENT:SASL_SSL,INTERNAL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.client.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="{admin}";
ssl.keystore.type=PEM
ssl.keystore.location=/etc/streamforge/kafka-keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/streamforge/ca.pem
listener.name.controller.ssl.client.auth=required
ssl.endpoint.identification.algorithm=https
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:CN={host}
allow.everyone.if.no.acl.found=false
auto.create.topics.enable=false
log.dirs=/var/lib/kafka/data
num.partitions=12
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
min.insync.replicas=1
unclean.leader.election.enable=false
log.retention.hours=168
'''
 write('server.properties',props,1000)
 write('admin.properties',f'security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="{admin}";\nssl.truststore.type=PEM\nssl.truststore.location=/etc/streamforge/ca.pem\n',1000)
 write('pg.key',secret['postgres_key_pem'],999);write('pg.crt',secret['postgres_cert_pem'],999);write('pg-admin-password',secret['postgres_passwords']['admin'],999)
 write('pg_hba.conf','local all all trust\nhostssl streamforge sf_consumer 10.42.0.0/16 scram-sha-256\nhostssl streamforge sf_replay 10.42.0.0/16 scram-sha-256\nhost all all 0.0.0.0/0 reject\n',999)
 if not a.render_only:
  for name,uid in [('kafka',1000),('postgres',999)]:path=Path('/data')/name;path.mkdir(exist_ok=True);os.chown(path,uid,uid)
  if not Path('/data/kafka/meta.properties').exists():
   opts=[]
   for user,password in secret['kafka_users'].items():opts+=['--add-scram',f'SCRAM-SHA-512=[name={user},password={password}]']
   docker(['run','--rm','--network','none','--user','1000:1000','-v','/data/kafka:/var/lib/kafka/data','-v',str(root)+':/etc/streamforge:ro','--entrypoint','/opt/kafka/bin/kafka-storage.sh',images['kafka'],'format','-t',secret['kafka_cluster_id'],'-c','/etc/streamforge/server.properties',*opts])
  start('streamforge-kafka',images['kafka'],['--memory','2g','--user','1000:1000','-e','KAFKA_HEAP_OPTS=-Xms512m -Xmx512m','-v','/data/kafka:/var/lib/kafka/data','-v',str(root)+':/etc/streamforge:ro','--entrypoint','/opt/kafka/bin/kafka-server-start.sh'],['/etc/streamforge/server.properties'])
  start('streamforge-postgres',images['postgres'],['--memory','512m','--user','999:999','-e','POSTGRES_USER=streamforge_admin','-e','POSTGRES_DB=streamforge','-e','POSTGRES_PASSWORD_FILE=/etc/streamforge/pg-admin-password','-v','/data/postgres:/var/lib/postgresql/data','-v',str(root)+':/etc/streamforge:ro'],['postgres','-c','ssl=on','-c','ssl_cert_file=/etc/streamforge/pg.crt','-c','ssl_key_file=/etc/streamforge/pg.key','-c','hba_file=/etc/streamforge/pg_hba.conf','-c','ssl_min_protocol_version=TLSv1.2'])
  def pg(sql):return docker(['exec','-i','streamforge-postgres','psql','-v','ON_ERROR_STOP=1','-U','streamforge_admin','-d','streamforge'],input=sql)
  for _ in range(90):
   try:
    if not steady('streamforge-postgres','postgres'):time.sleep(2);continue
    pg('SELECT 1;');break
   except CommandFailure:time.sleep(2)
  else:raise RuntimeError('PostgreSQL never became ready')
  initial_schema('postgres',schema,pg)
  for user in ['consumer','replay']:
   password=secret['postgres_passwords'][user]
   if not password.replace('_','').replace('-','').isalnum():raise RuntimeError('unsafe generated password format')
   pg(f"DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='sf_{user}') THEN CREATE ROLE sf_{user} LOGIN PASSWORD '{password}'; END IF; END $$; GRANT CONNECT ON DATABASE streamforge TO sf_{user}; GRANT USAGE ON SCHEMA public TO sf_{user};")
  pg('GRANT SELECT,INSERT,UPDATE ON dlq_audit TO sf_consumer; GRANT SELECT,INSERT,UPDATE ON tenants,replay_jobs,replay_audit TO sf_replay;')
  def kafka(tool,args):return docker(['exec','streamforge-kafka','/opt/kafka/bin/'+tool,'--bootstrap-server',host+':9094','--command-config','/etc/streamforge/admin.properties',*args])
  for _ in range(90):
   try:kafka('kafka-topics.sh',['--list']);break
   except CommandFailure:time.sleep(2)
  else:raise RuntimeError('Kafka never became ready')
  for topic,size in [('streamforge.events.v1','1048576'),('streamforge.events.dlq.v1','2097152')]:kafka('kafka-topics.sh',['--create','--if-not-exists','--topic',topic,'--partitions','12','--replication-factor','1','--config','retention.ms=604800000','--config','max.message.bytes='+size])
  for user,topic,operations in [('ingest','streamforge.events.v1',['Write','Describe']),('consumer','streamforge.events.v1',['Read','Describe']),('consumer','streamforge.events.dlq.v1',['Write','Describe']),('replay','streamforge.events.dlq.v1',['Read','Describe']),('replay','streamforge.events.v1',['Write','Describe'])]:
   args=['--add','--allow-principal','User:'+user,'--topic',topic]
   for operation in operations:args+=['--operation',operation]
   kafka('kafka-acls.sh',args)
  kafka('kafka-acls.sh',['--add','--allow-principal','User:consumer','--group','streamforge-sink-v1','--operation','Read','--operation','Describe'])
  for user in ['ingest','consumer','replay']:kafka('kafka-acls.sh',['--add','--allow-principal','User:'+user,'--cluster','--operation','IdempotentWrite'])
else:
 write('ca.pem',secret['ca_pem'],101);write('ch.key',secret['key_pem'],101);write('ch.crt',secret['cert_pem'],101)
 config=ET.Element('clickhouse');ET.SubElement(config,'listen_host').text='0.0.0.0';ET.SubElement(config,'http_port',remove='1');ET.SubElement(config,'tcp_port',remove='1');ET.SubElement(config,'https_port').text='8443';ET.SubElement(config,'tcp_port_secure').text='9440'
 ssl=ET.SubElement(ET.SubElement(config,'openSSL'),'server')
 for key,value in {'certificateFile':'/etc/streamforge/ch.crt','privateKeyFile':'/etc/streamforge/ch.key','verificationMode':'none','disableProtocols':'sslv2,sslv3,tlsv1,tlsv1_1'}.items():ET.SubElement(ssl,key).text=value
 ET.SubElement(config,'max_server_memory_usage_to_ram_ratio').text='0.8';ET.SubElement(config,'mark_cache_size').text='134217728';ET.SubElement(config,'uncompressed_cache_size').text='67108864'
 write('secure.xml',ET.tostring(config,encoding='unicode'),101)
 users=ET.Element('clickhouse');u=ET.SubElement(users,'users')
 for name,password in {'admin':secret['passwords']['admin']}.items():
  node=ET.SubElement(u,'sf_'+name);ET.SubElement(node,'password_sha256_hex').text=hashlib.sha256(password.encode()).hexdigest();ET.SubElement(node,'profile').text='default';ET.SubElement(node,'quota').text='default';ET.SubElement(ET.SubElement(node,'networks'),'ip').text='10.42.0.0/16'
  if name=='admin':ET.SubElement(node,'access_management').text='1';ET.SubElement(ET.SubElement(node,'networks'),'ip').text='127.0.0.1'
 ET.SubElement(u,'default',remove='1');write('users.xml',ET.tostring(users,encoding='unicode'),101)
 if not a.render_only:
  path=Path('/data/clickhouse');path.mkdir(exist_ok=True);os.chown(path,101,101)
  start('streamforge-clickhouse',images['clickhouse'],['--memory','12g','--user','101:101','-e','CLICKHOUSE_SKIP_USER_SETUP=1','--ulimit','nofile=262144:262144','-v','/data/clickhouse:/var/lib/clickhouse','-v',str(root)+':/etc/streamforge:ro','-v',str(root/'secure.xml')+':/etc/clickhouse-server/config.d/secure.xml:ro','-v',str(root/'users.xml')+':/etc/clickhouse-server/users.d/secure.xml:ro'])
  client=ET.Element('config');ET.SubElement(client,'user').text='sf_admin';ET.SubElement(client,'password').text=secret['passwords']['admin']
  tls=ET.SubElement(ET.SubElement(client,'openSSL'),'client');ET.SubElement(tls,'caConfig').text='/etc/streamforge/ca.pem';ET.SubElement(tls,'verificationMode').text='strict';ET.SubElement(tls,'loadDefaultCAFile').text='false'
  write('client.xml',ET.tostring(client,encoding='unicode'),101)
  def ch(sql):return docker(['exec','-i','streamforge-clickhouse','clickhouse-client','--config-file','/etc/streamforge/client.xml','--host','127.0.0.1','--secure','--port','9440','--multiquery'],input=sql)
  for _ in range(90):
   try:
    if not steady('streamforge-clickhouse','clickhouse'):time.sleep(2);continue
    ch('SELECT 1;');break
   except CommandFailure:time.sleep(2)
  else:raise RuntimeError('ClickHouse never became ready')
  ch('CREATE DATABASE IF NOT EXISTS streamforge;')
  initial_schema('clickhouse',schema,lambda sql:ch('USE streamforge;\n'+sql))
  for view in ['revenue_1m_mv','traffic_1m_mv','event_identity_values_mv']:ch('USE streamforge; ALTER TABLE '+view+' MODIFY SQL SECURITY DEFINER DEFINER=CURRENT_USER;')
  for user in ['consumer','query']:
   digest=hashlib.sha256(secret['passwords'][user].encode()).hexdigest()
   ch(f"CREATE USER IF NOT EXISTS sf_{user} IDENTIFIED WITH sha256_hash BY '{digest}' HOST IP '10.42.0.0/16';")
  ch('GRANT INSERT ON streamforge.events_raw TO sf_consumer; GRANT SELECT(event_id) ON streamforge.event_identity_values TO sf_consumer; GRANT SELECT ON streamforge.revenue_1m_canonical TO sf_query; GRANT SELECT ON streamforge.traffic_1m_canonical TO sf_query; GRANT SELECT ON system.view_refreshes TO sf_query; GRANT SHOW TABLES ON streamforge.* TO sf_query;')
print('Stateful configuration completed; no secret values logged.')
