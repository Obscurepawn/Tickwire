import datetime,hashlib,json,os,subprocess,sys,time
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');bins=Path('/tmp/tw-process-resources-bins-1')
case=sys.argv[1];root=Path('/tmp/tw-res-'+case+'-1');prefix='process-resources-'+case
manifest=json.loads((tmp/'process-resources-binaries.json').read_text())
def hashes():
 return {r['name']:hashlib.sha256(Path(r['deployed_path']).read_bytes()).hexdigest() for r in manifest['binaries']}
expected={r['name']:r['deployed_sha256'] for r in manifest['binaries']};before=hashes();assert before==expected
def verify(stage):
 with (tmp/f'{prefix}-source-{stage}.log').open('w') as f:subprocess.run(['python3',str(tmp/'process-resources-source-manifest.py'),'--verify'],check=True,stdout=f)
verify('before')
started=datetime.datetime.now(datetime.timezone.utc).isoformat();start=time.monotonic_ns();scenario='spot-'+case+'-custom'
gen=[str(bins/'tickwire-lab'),'generate','--scenario',scenario,'--out',str(root),'--bin-dir',str(bins)]
with (tmp/f'{prefix}-generate.log').open('w') as f:gcode=subprocess.call(gen,cwd=repo,stdout=f,stderr=subprocess.STDOUT)
assert gcode==0
cmd=[str(bins/'tickwire-lab'),'run','--config',str(root/'scenario.json')];sample=None
with (tmp/f'{prefix}-run.log').open('w') as f:
 p=subprocess.Popen(cmd,cwd=repo,stdout=f,stderr=subprocess.STDOUT)
 until=time.monotonic()+180
 while p.poll() is None:
  if sample is None:
   try:
    proof=json.loads((root/'handover/preflight-verified.json').read_text());pid=proof['target_pid'];proc=Path(f'/proc/{pid}');raw=(proc/'exe').read_bytes()
    sample={'pid':pid,'exe_link':os.readlink(proc/'exe'),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'stat':(proc/'stat').read_text().strip(),'sampled_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
   except (OSError,KeyError,ValueError):pass
  if time.monotonic()>until:p.kill();p.wait();raise RuntimeError('bounded scenario timeout')
  time.sleep(.1)
 code=p.returncode
verify('after');after=hashes();assert after==before
result=json.loads((root/'run-result.json').read_text()) if (root/'run-result.json').exists() else None
meta={'schema':1,'scenario':scenario,'root':str(root),'binary_manifest':str(tmp/'process-resources-binaries.json'),'started_utc':started,'started_monotonic_ns':start,'generate_command':gen,'generate_exit_code':gcode,'run_command':cmd,'run_exit_code':code,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'completed_monotonic_ns':time.monotonic_ns(),'binary_hashes_before':before,'binary_hashes_after':after,'binary_hashes_unchanged':True,'source_manifest_verified':True,'result':result}
(tmp/f'{prefix}-metadata.json').write_text(json.dumps(meta,indent=2)+'\n');(tmp/f'{prefix}-live-images.json').write_text(json.dumps({'images':[sample] if sample else []},indent=2)+'\n')
print(json.dumps({'scenario':scenario,'code':code,'seconds':(meta['completed_monotonic_ns']-start)/1e9,'result':result,'image':sample}))
assert code==0 and result is not None and result['account_fill_verified'] and sample is not None
assert sample['sha256']==expected['tickwire-counting-worker']
