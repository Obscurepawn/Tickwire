import datetime,json,os,subprocess,time
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');worker='/tmp/tw-param-bins-1/tickwire-counting-worker'
verify=['python3',str(tmp/'strategy-parameters-source-manifest.py'),'--verify'];subprocess.run(verify,check=True)
cmd=['cargo','test','-p','tickwire-strategy-host','--test','capture_process','actual_fresh_queries_cross_the_report_floor_and_keep_callbacks_running','--','--ignored','--nocapture']
start=time.monotonic();started=datetime.datetime.now(datetime.timezone.utc).isoformat()
with (tmp/'strategy-parameters-host-process.log').open('w') as f:code=subprocess.call(cmd,cwd=repo,env=dict(os.environ,CARGO_TARGET_DIR=str(tmp/'target'),TICKWIRE_CAPTURE_WORKER=worker),stdout=f,stderr=subprocess.STDOUT)
subprocess.run(verify,check=True)
meta={'command':cmd,'worker':worker,'started_utc':started,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-start,'exit_code':code}
(tmp/'strategy-parameters-host-process-meta.json').write_text(json.dumps(meta,indent=2)+'\n');print(json.dumps(meta));raise SystemExit(code)
