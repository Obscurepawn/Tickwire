#!/usr/bin/env python3
"""Real App/MD/worker file-observation and Spot fee-closure acceptance.
Builds no product state, does not synthesize live proofs; generated lab config is explicit.
Usage: python3 runtime-observation-app-driver.py --bins /fixed/bins --root /new/case --output /evidence.json --verifier /actual/verifier [--recover]
"""
import argparse, copy, hashlib, json, os, shutil, signal, subprocess, sys, time
from pathlib import Path
ap=argparse.ArgumentParser();ap.add_argument('--bins',type=Path,required=True);ap.add_argument('--root',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);ap.add_argument('--verifier',type=Path,required=True);ap.add_argument('--recover',action='store_true');a=ap.parse_args()
repo=Path('/home/obscure/Tickwire');root=a.root;bins=a.bins
assert root.is_absolute() and bins.is_absolute() and not root.exists()
assert len(os.fsencode(root/'host/runtime'/('tickwire-strategy-'+'0'*32)/'intent.sock')) < 108, 'choose a shorter fixture root for the actual Unix socket path'
raw_now=lambda:time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
sha=lambda b:hashlib.sha256(b).hexdigest()
compact=lambda v:json.dumps(v,separators=(',',':'),ensure_ascii=False).encode()
def atomic(path,data):
 path=Path(path);tmp=path.with_name('.'+path.name+'.pending');fd=os.open(tmp,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 try:
  with os.fdopen(fd,'wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
  os.replace(tmp,path);d=os.open(path.parent,os.O_DIRECTORY);os.fsync(d);os.close(d)
 finally:
  if tmp.exists():tmp.unlink()
def readj(p):return json.loads(Path(p).read_bytes())
def wait(fn,seconds=20,process=None,label='condition'):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  try:
   x=fn()
   if x:return x
  except (FileNotFoundError,KeyError,json.JSONDecodeError,ProcessLookupError):pass
  if process is not None and process.poll() is not None:raise AssertionError(f'{label}: process exited {process.returncode}')
  time.sleep(.01)
 raise AssertionError('timeout: '+label)
def proc_sample(pid):
 p=Path(f'/proc/{pid}');stat=(p/'stat').read_bytes();fields=stat[stat.rindex(b')')+2:].split();image=(p/'exe').read_bytes();status=(p/'status').read_bytes()
 vals={line.split(b':',1)[0]:line.split(b':',1)[1].strip() for line in status.splitlines() if b':' in line}
 return {'pid':pid,'start_ticks':int(fields[19]),'image_sha256':sha(image),'image_bytes':len(image),'exe':os.readlink(p/'exe'),'uid':int(vals[b'Uid'].split()[0]),'gid':int(vals[b'Gid'].split()[0]),'sampled_mono_ns':raw_now()}
meta={'schema':1,'scope':'actual App, independent MD and supervised worker; loopback TLS Spot fill plus fees','root':str(root),'binaries':{},'started_mono_ns':raw_now(),'command_line':sys.argv,'driver_sha256':sha(Path(__file__).read_bytes()),'observations':[],'negative_results':[]}
for path in sorted(bins.glob('tickwire-*')):
 if path.is_file():meta['binaries'][path.name]={'path':str(path),'sha256':sha(path.read_bytes())}
a.output.parent.mkdir(parents=True,exist_ok=True)
run=None;recovery=None
try:
 generated=subprocess.run([str(bins/'tickwire-lab'),'generate','--scenario','spot-fill','--out',str(root),'--bin-dir',str(bins)],cwd=repo,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 (a.output.parent/(a.output.stem+'-generate.log')).write_bytes(generated.stdout);assert generated.returncode==0
 app_path=root/'exec/config.json';app=readj(app_path);bootstrap=readj(root/'strategy/bootstrap.json');identity=bootstrap['report']['local'];worker=Path(app['strategy']['config']['executable']);build=hashlib.sha256(worker.read_bytes()).digest()
 dirs={n:root/('observation-'+n) for n in ['inbox','outbox','journal']}
 for p in dirs.values():p.mkdir(mode=0o700)
 app['observation']={'inbox_directory':str(dirs['inbox']),'outbox_directory':str(dirs['outbox']),'journal_directory':str(dirs['journal']),'stream_epoch':[199]*16,'owner_uid':os.getuid(),'allowed_build_hashes':[list(build)],'maximum_requests':64,'maximum_pending':4,'maximum_per_poll':8,'recover':False}
 app['maximum_runtime_ns']=45_000_000_000
 app['stop_file']=str(root/'APP_STOP')
 atomic(app_path,compact(app))
 # This generated kind-9 recovery dependency binds the deliberately enabled deployment.
 dep=json.dumps(app,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode();dep_path=root/'strategy/dependencies/9.bin';atomic(dep_path,dep)
 template_path=root/'strategy/recovery-template.json';template=readj(template_path)
 for d in template['dependencies']:
  if d['kind']==9:d['digest']=list(hashlib.sha256(dep).digest())
 atomic(template_path,compact(template))
 log=(a.output.parent/(a.output.stem+'-run.log')).open('wb');run=subprocess.Popen([str(bins/'tickwire-lab'),'run','--config',str(root/'scenario.json')],cwd=repo,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 session=wait(lambda:readj(dirs['outbox']/'session.json'),process=run,label='real observation session')
 atomic(root/'initial-observation-session.json',(dirs['outbox']/'session.json').read_bytes())
 target={'logical_strategy_id':identity['logical_strategy'],'instance':identity['instance'],'generation':identity['generation']}
 consumed=wait(lambda:next((readj(p) for p in (root/'exec/reports').glob('CONSUMED-*.json')),None),process=run,label='real worker consumption')
 pid=consumed['worker_pid'];image=proc_sample(pid);assert image['image_sha256']==build.hex();meta['worker_before']=image;meta['session']=session
 def request(n,**updates):
  q={'schema':1,'request_id':list(n.to_bytes(16,'little')),'session_id':session['session_id'],'binding':session['binding'],'target':target.copy(),'valid_until_mono_ns':raw_now()+20_000_000_000,'require_resources':False};q.update(updates);return q
 def submit(q,expected='observed'):
  name=bytes(q['request_id']).hex()+'.json';out=dirs['outbox']/name;atomic(dirs['inbox']/name,compact(q))
  result=wait(lambda:readj(out),process=run,label='actual outcome '+name)
  assert result['request']==q,(result,q);assert result['status']==expected,result;assert not result['historical'];assert result['request_digest']==list(hashlib.sha256(compact(q)).digest())
  snapshot=root/('outcome-'+name);atomic(snapshot,compact(result))
  if expected=='observed':
   o=result['observation'];p=o['process'];current=proc_sample(p['pid']);assert current['pid']==pid and current['start_ticks']==image['start_ticks'] and current['image_sha256']==build.hex();assert p['build_hash']==list(build) and p['image_bytes']==image['image_bytes'];assert p['uid']==image['uid'] and p['gid']==image['gid'];assert o['account_position']>=o['report_sequence_floor'];assert any(o['challenge']);assert o['started_mono_ns']<=o['observed_mono_ns']<=result['completed_mono_ns']<q['valid_until_mono_ns'];assert raw_now()<q['valid_until_mono_ns'];assert o['checkpoint'] in o['objects'] and o['input_seal'] in o['objects']
   for ref in o['objects']:
    b=(dirs['journal']/('object-'+bytes(ref['digest']).hex())).read_bytes();assert len(b)==ref['bytes'] and hashlib.sha256(b).digest()==bytes(ref['digest'])
   meta['observations'].append({'outcome':str(snapshot),'request_id':bytes(q['request_id']).hex(),'pid':pid,'challenge':bytes(o['challenge']).hex(),'ready_sequence':o['ready_sequence'],'account_position':o['account_position'],'floor':o['report_sequence_floor'],'objects':len(o['objects']),'sample':current})
  else:meta['negative_results'].append({'request_id':bytes(q['request_id']).hex(),'status':expected,'outcome':str(snapshot)})
  return result
 q1=request(1);r1=submit(q1);time.sleep(.2);q2=request(2);r2=submit(q2)
 assert r1['observation']['challenge']!=r2['observation']['challenge'];assert r2['observation']['ready_sequence']>r1['observation']['ready_sequence']
 same=dirs['outbox']/(bytes(q2['request_id']).hex()+'.json');before=same.read_bytes();atomic(dirs['inbox']/same.name,compact(q2));time.sleep(.15);assert same.read_bytes()==before;meta['identical_request_no_new_result']=True;meta['duplicate_identical']={'before_sha256':sha(before),'after_sha256':sha(same.read_bytes()),'outcome':meta['observations'][1]['outcome']}
 bad=target.copy();bad['generation']+=999;submit(request(3,target=bad),'rejected');submit(request(4,session_id=[198]*16),'rejected');submit(request(5,valid_until_mono_ns=raw_now()-1),'expired');submit(request(6,require_resources=True),'unsupported')
 conflict=copy.deepcopy(q2);conflict['valid_until_mono_ns']+=1;atomic(dirs['inbox']/same.name,compact(conflict));c=wait(lambda:(v if (v:=readj(same))['status']=='rejected' else None),process=run,label='conflicting ID rejection');assert c['request']==conflict;meta['conflicting_completed_id_rejected']=True;atomic(root/'outcome-conflict.json',same.read_bytes());meta['conflict_outcome']=str(root/'outcome-conflict.json')
 # The actual fee-maintenance REST traffic must occur before requesting process stop.
 wait(lambda:any(v.get('event')=='http' and v.get('path')=='/api/v3/myTrades' and v.get('status')==200 for v in [json.loads(l) for l in (root/'fixture.jsonl').read_text().splitlines()]),process=run,label='actual fee query')
 wait(lambda:next((v for path in (root/'exec/reports').glob('CONSUMED-*.json') if (v:=readj(path))['ledger_sequence']>=8 and v['account_position']>=r2['observation']['account_position']),None),process=run,label='actual final worker fee prefix')
 # Replication is asynchronous: require a validated durable mirror prefix before stopping.
 mirror_attempt=0
 def mirror_ready():
  global mirror_attempt
  mirror_attempt+=1;scratch=root/('mirror-prefix-check-'+str(mirror_attempt))
  result=subprocess.run([str(a.verifier),'--mirror-prefix',str(root/'mirror/config.json'),str(scratch)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  try:
   if result.returncode!=0:return None
   proof=json.loads(result.stdout)
   consumed=next(readj(path) for path in (root/'exec/reports').glob('CONSUMED-*.json'))
   return proof if proof['prefix']['sequence']>=consumed['account_position'] and proof['prefix']['sequence']>=8 else None
  finally:shutil.rmtree(scratch)
 meta['mirror_before_stop']=wait(mirror_ready,seconds=20,process=run,label='actual durable mirror full prefix')
 meta['worker_after']=proc_sample(pid);assert meta['worker_after']['start_ticks']==image['start_ticks']
 pending=None
 if a.recover:
  # An actual stopped worker leaves a genuinely admitted request unresolved.
  os.kill(pid,signal.SIGSTOP)
  wait(lambda:Path(f'/proc/{pid}/stat').read_bytes().rsplit(b')',1)[1].split()[0]==b'T',label='actual stopped worker')
  pending=request(7);atomic(dirs['inbox']/(bytes(pending['request_id']).hex()+'.json'),compact(pending))
  wait(lambda:any(compact(pending) in path.read_bytes() for path in dirs['journal'].glob('segment-*.wal')),process=run,label='actual durable Received before stop')
  assert not (dirs['outbox']/(bytes(pending['request_id']).hex()+'.json')).exists()
  meta['unfinished_request']=pending
 atomic(root/'APP_STOP',b'observation acceptance complete\n');code=run.wait(timeout=30);log.close();assert code==0,code
 meta['run_exit_code']=code;meta['business']=readj(root/'run-result.json');assert meta['business']['account_fill_verified'];meta['account_report']=readj(root/'account-report.json');assert meta['account_report']['orders'][0]['fee_reconciled']
 for n,o in enumerate(meta['observations']):
  v=subprocess.run([str(a.verifier),o['outcome'],str(dirs['journal']),str(root/'export'),str(root/f'closure-check-{n}')],stdout=subprocess.PIPE,stderr=subprocess.PIPE);assert v.returncode==0,v.stderr.decode();o['semantic_verification']=json.loads(v.stdout)
 if a.recover:
  recovered=copy.deepcopy(app);recovered['observation']['recover']=True;recovered['control']['recover']=True;recovered['recover']=True;recovered['replication']=None;recovered['strategy']={'mode':'observe'};recovered['market_sources']=[];recovered['maximum_runtime_ns']=3_000_000_000;recovered['stop_file']=str(root/'RECOVERY_STOP');recovered['first_connection_epoch']=10000;recovered['reconciliation']['first_execution_connection_epoch']=10001;recovered['tuning']['connect_timeout_ns']=100_000_000;recovered['tuning']['request_timeout_ns']=100_000_000
  rd=root/'recovery-check';rd.mkdir(mode=0o700)
  for key,path in recovered['directories'].items():
   dest=rd/key;shutil.copytree(path,dest);recovered['directories'][key]=str(dest)
  cc=root/'recovery-config.json';atomic(cc,compact(recovered));recover_log=(a.output.parent/(a.output.stem+'-recovery.log')).open('wb');recovery=subprocess.Popen([str(bins/'tickwire-app'),'run','--config',str(cc)],cwd=repo,stdout=recover_log,stderr=subprocess.STDOUT,start_new_session=True)
  second=wait(lambda:(s if (s:=readj(dirs['outbox']/'session.json'))['session_number']==2 else None),process=recovery,label='recovered real App session')
  old=readj(dirs['outbox']/(bytes(q1['request_id']).hex()+'.json'));assert old['historical'] and old['observation']==r1['observation'] and old['request']==q1;assert second['session_id']!=session['session_id'];atomic(root/'outcome-historical.json',compact(old));assert not Path(f'/proc/{pid}').exists()
  interrupted=readj(dirs['outbox']/(bytes(pending['request_id']).hex()+'.json'));assert interrupted['historical'] and interrupted['status']=='interrupted' and interrupted['request']==pending and interrupted['observation'] is None
  atomic(root/'outcome-interrupted.json',compact(interrupted))
  meta['recovery']={'unfinished_request_interrupted_without_redispatch':True,'interrupted_outcome':str(root/'outcome-interrupted.json'),'historical_outcome':str(root/'outcome-historical.json'),'session':second,'original_observed_republished_historical':True,'original_challenge_preserved':True,'original_worker_exited':True,'configuration':'Observe strategy; cloned account WAL; same actual observation WAL; no worker relaunch or live inference'}
  code=recovery.wait(timeout=15);recover_log.close();meta['recovery']['exit_code']=code;assert code==0,code
 meta['completed_mono_ns']=raw_now();meta['passed']=True;meta['status']='verified'
 meta['observation_config']=app['observation']
 audit_path=a.output.with_name(a.output.stem+'-closure-audit.json')
 audit={'schema':1,'scope':'Live CAS was fully semantically verified before archival; archive omits ELF and large CAS. Hash lists and these run-time audit results do not reconstruct omitted objects.','verifier_sha256':sha(a.verifier.read_bytes()),'observations':[]}
 for item in meta['observations']:
  full=readj(item['outcome']);audit['observations'].append({'request_id':item['request_id'],'objects':full['observation']['objects'],'verified':item['semantic_verification']})
 audit_path.write_text(json.dumps(audit,indent=2)+'\n')
 artifacts={'driver.py':str(Path(__file__).resolve()),'verifier-main.rs':'/tmp/tickwire-observation-verifier/src/main.rs','verifier-Cargo.toml':'/tmp/tickwire-observation-verifier/Cargo.toml','verifier-Cargo.lock':'/tmp/tickwire-observation-verifier/Cargo.lock','closure-audit.json':str(audit_path),'run-result.json':str(root/'run-result.json'),'account-report.json':str(root/'account-report.json'),'exec.stderr.log':str(root/'logs/exec.stderr.log'),'fixture.jsonl':str(root/'fixture.jsonl'),'session.json':str(dirs['outbox']/'session.json'),'initial-session.json':str(root/'initial-observation-session.json'),'outcomes/conflict.json':meta['conflict_outcome'],'business-baseline.cbor':str(root/'export/baseline.cbor')}
 for path in sorted((root/'export/records').glob('*.cbor')):artifacts['business-records/'+path.name]=str(path)
 for item in meta['observations']+meta['negative_results']:artifacts['outcomes/'+Path(item['outcome']).name]=item['outcome']
 for path in sorted(dirs['journal'].iterdir()):
  if path.name=='MANIFEST.json' or path.suffix=='.wal':artifacts['observation-wal/'+path.name]=str(path)
  elif path.name.startswith('object-') and path.stat().st_size<=256*1024:
   try:
    value=readj(path)
    if isinstance(value,dict) and 'request_digest' in value and 'historical' in value:artifacts['observation-results/'+path.name+'.json']=str(path)
   except (ValueError,UnicodeDecodeError):pass
 if a.recover:
  artifacts['outcomes/interrupted.json']=meta['recovery']['interrupted_outcome'];artifacts['outcomes/historical.json']=meta['recovery']['historical_outcome']
 for suffix in ['generate','run','recovery']:
  path=a.output.parent/(a.output.stem+'-'+suffix+'.log')
  if path.exists():artifacts[suffix+'.log']=str(path)
 meta['artifacts']=artifacts
 meta['artifact_sha256']={name:sha(Path(path).read_bytes()) for name,path in artifacts.items()}
 for name,m in meta['binaries'].items():assert sha(Path(m['path']).read_bytes())==m['sha256']
except BaseException as e:
 meta['passed']=False;meta['error']=repr(e);raise
finally:
 for p in [recovery,run]:
  if p is not None and p.poll() is None:os.killpg(p.pid,signal.SIGTERM);p.wait(timeout=15)
 a.output.write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps({'passed':meta['passed'],'root':str(root),'output':str(a.output),'observations':meta['observations'],'negative_results':meta['negative_results'],'fee_closed':meta['account_report']['orders'][0]['fee_reconciled']}))
