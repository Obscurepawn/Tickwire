import datetime, hashlib, json, os, subprocess, time
from pathlib import Path
repo=Path('/home/obscure/Tickwire'); tmp=Path('/tmp/tickwire-progress-20261028')
binary=Path('/tmp/tw-md-restart-bins-1/tickwire-md')
manifest=json.loads((tmp/'md-restart-binaries.json').read_text())
expected=next(x['deployed_sha256'] for x in manifest['binaries'] if x['name']=='tickwire-md')
assert hashlib.sha256(binary.read_bytes()).hexdigest()==expected
verify=['python3',str(tmp/'md-restart-source-manifest.py'),'--verify']
subprocess.run(verify,check=True,cwd=repo)
cmd=['cargo','test','-p','tickwire-md','--lib','server::runtime_tests','--','--nocapture']
env=dict(os.environ,CARGO_TARGET_DIR=str(tmp/'target'),TICKWIRE_MD_RUNTIME_TEST_BINARY=str(binary))
started=datetime.datetime.now(datetime.timezone.utc).isoformat(); begin=time.monotonic_ns(); samples={}
with (tmp/'md-restart-cli-acceptance.log').open('w') as log:
 process=subprocess.Popen(cmd,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
 while process.poll() is None:
  for entry in Path('/proc').iterdir():
   if not entry.name.isdigit() or entry.name in samples: continue
   try:
    if os.readlink(entry/'exe')!=str(binary): continue
    raw=(entry/'exe').read_bytes()
    samples[entry.name]={'pid':int(entry.name),'exe_link':os.readlink(entry/'exe'),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'stat':(entry/'stat').read_text().strip(),'sampled_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
   except (OSError,ProcessLookupError): pass
  if time.monotonic_ns()-begin>180_000_000_000:
   process.kill();process.wait();raise RuntimeError('bounded CLI acceptance timeout')
  time.sleep(.01)
 code=process.returncode
subprocess.run(verify,check=True,cwd=repo)
assert hashlib.sha256(binary.read_bytes()).hexdigest()==expected
rows=[]
for line in (tmp/'md-restart-cli-acceptance.log').read_text().splitlines():
 if line.startswith('{'):
  try: rows.append(json.loads(line))
  except ValueError: pass
positive=[r for r in rows if r.get('event')=='md_cold_restart_verified']
meta={'command':cmd,'environment':{key:env[key] for key in ['CARGO_TARGET_DIR','TICKWIRE_MD_RUNTIME_TEST_BINARY']},'binary':str(binary),'binary_sha256':expected,'started_utc':started,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':(time.monotonic_ns()-begin)/1e9,'exit_code':code,'source_and_binary_unchanged':True,'events':rows,'samples':list(samples.values())}
(tmp/'md-restart-cli-acceptance.json').write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps({'code':code,'seconds':meta['elapsed_seconds'],'actual_image_samples':len(samples),'positive_runs':len(positive)}))
assert code==0 and len(positive)==1
for key in ['old_pid','new_pid']:
 sample=samples[str(positive[0][key])];assert sample['sha256']==expected
fault=[r for r in rows if r.get('event')=='md_starting_fault_recovered'];assert len(fault)==1
assert samples[str(fault[0]['pid'])]['sha256']==expected
assert {r['phase'] for r in rows if r.get('event')=='md_incomplete_restart_rejected'}=={'starting','reserved','revoking','applied'}
assert positive[0]['revision']==3 and positive[0]['instrument_ids']==[2**64-1,2**63+1001,2**64-2]
