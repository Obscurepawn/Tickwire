import datetime,hashlib,json,os,subprocess,time
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');worker=Path('/tmp/tw-process-resources-bins-1/tickwire-counting-worker')
verify=['python3',str(tmp/'process-resources-source-manifest.py'),'--verify']
with (tmp/'process-resources-host-source-before.log').open('w') as f:subprocess.run(verify,check=True,stdout=f)
digest=hashlib.sha256(worker.read_bytes()).hexdigest();start=time.monotonic();started=datetime.datetime.now(datetime.timezone.utc).isoformat()
env={'CARGO_TARGET_DIR':str(tmp/'target'),'TICKWIRE_CAPTURE_WORKER':str(worker),'TICKWIRE_CAPTURE_EVIDENCE':str(tmp/'process-resources-capture-evidence'),'TICKWIRE_REBUILD_EVIDENCE':str(tmp/'process-resources-rebuild-evidence')}
assert not Path(env['TICKWIRE_CAPTURE_EVIDENCE']).exists() and not Path(env['TICKWIRE_REBUILD_EVIDENCE']).exists()
cmd=['cargo','test','--locked','-p','tickwire-strategy-host','--test','capture_process','--','--ignored','--nocapture']
with (tmp/'process-resources-host-process.log').open('w') as f:code=subprocess.call(cmd,cwd=repo,env=dict(os.environ,**env),stdout=f,stderr=subprocess.STDOUT)
with (tmp/'process-resources-host-source-after.log').open('w') as f:subprocess.run(verify,check=True,stdout=f)
after=hashlib.sha256(worker.read_bytes()).hexdigest();assert after==digest
meta={'command':cmd,'environment':env,'started_utc':started,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-start,'exit_code':code,'worker_sha256':digest,'worker_sha256_after':after,'worker_unchanged':True,'source_manifest_verified':True}
(tmp/'process-resources-host-process-metadata.json').write_text(json.dumps(meta,indent=2)+'\n');print(json.dumps(meta));assert code==0
