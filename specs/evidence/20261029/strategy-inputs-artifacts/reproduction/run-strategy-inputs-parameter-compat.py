import datetime,hashlib,json,os,subprocess,time
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');bins=Path('/tmp/tw-input-bins-1')
prefix='strategy-inputs-parameter-compat'
env=dict(os.environ,CARGO_TARGET_DIR=str(tmp/'target'),TICKWIRE_PARAMETERS_WORKER=str(bins/'tickwire-counting-worker'),TICKWIRE_PARAMETERS_KEEP='1')
verify=['python3',str(tmp/'strategy-inputs-source-manifest.py'),'--verify']
subprocess.run(verify,check=True)
manifest=json.loads((tmp/'strategy-inputs-binaries.json').read_text())
def hashes():return {r['name']:hashlib.sha256(Path(r['deployed_path']).read_bytes()).hexdigest() for r in manifest['binaries']}
expected={r['name']:r['deployed_sha256'] for r in manifest['binaries']};assert hashes()==expected
build=['cargo','test','-p','tickwire-app','--test','parameters_process','--no-run','--message-format=json']
with (tmp/f'{prefix}-build.jsonl').open('w') as out,(tmp/f'{prefix}-build.stderr').open('w') as err:
 result=subprocess.run(build,cwd=repo,env=env,stdout=out,stderr=err)
assert result.returncode==0
rows=[json.loads(line) for line in (tmp/f'{prefix}-build.jsonl').read_text().splitlines() if line.startswith('{')]
exe=next(r['executable'] for r in rows if r.get('reason')=='compiler-artifact' and r['target']['name']=='parameters_process' and r.get('executable'))
sha=lambda path:hashlib.sha256(Path(path).read_bytes()).hexdigest()
exe_before=sha(exe);started=datetime.datetime.now(datetime.timezone.utc).isoformat();start=time.monotonic();samples={};log=tmp/f'{prefix}.log';cmd=[exe,'--ignored','--nocapture']
with log.open('w') as f:
 p=subprocess.Popen(cmd,cwd=repo,env=env,stdout=f,stderr=subprocess.STDOUT)
 while p.poll() is None:
  for line in log.read_text(errors='replace').splitlines():
   try:row=json.loads(line)
   except ValueError:continue
   pid=row.get('worker_pid',row.get('pid'))
   if not pid or pid in samples:continue
   try:
    proc=Path(f'/proc/{pid}');link=os.readlink(proc/'exe');raw=(proc/'exe').read_bytes()
    if 'memfd:tickwire' not in link:continue
    samples[pid]={'pid':pid,'exe_link':link,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'stat':(proc/'stat').read_text().strip(),'sampled_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
   except OSError:pass
  if time.monotonic()-start>120:p.kill();p.wait();raise RuntimeError('process test timeout')
  time.sleep(.02)
 code=p.returncode
assert sha(exe)==exe_before
assert hashes()==expected
subprocess.run(verify,check=True)
meta={'command':cmd,'environment':{'TICKWIRE_PARAMETERS_WORKER':env['TICKWIRE_PARAMETERS_WORKER'],'TICKWIRE_PARAMETERS_KEEP':'1'},'test_executable_sha256':exe_before,'started_utc':started,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-start,'exit_code':code,'source_manifest_verified':True,'binary_hashes_before':expected,'binary_hashes_after':hashes(),'live_images':list(samples.values())}
(tmp/f'{prefix}-meta.json').write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps({'exit_code':code,'elapsed_seconds':meta['elapsed_seconds'],'live_images':len(samples)}));raise SystemExit(code)
