import datetime,json,os,subprocess,time
from pathlib import Path
root=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028')
start=time.monotonic();created=datetime.datetime.now(datetime.timezone.utc).isoformat()
env=dict(os.environ,CARGO_TARGET_DIR=str(tmp/'target'))
with (tmp/'md-restart-check-rust.log').open('w') as log:
 code=subprocess.call(['./scripts/check-rust.sh'],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
meta={'command':'CARGO_TARGET_DIR=/tmp/tickwire-progress-20261028/target ./scripts/check-rust.sh','started_utc':created,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-start,'exit_code':code}
(tmp/'md-restart-check-rust-meta.json').write_text(json.dumps(meta,indent=2)+'\n')
print(json.dumps(meta));raise SystemExit(code)
