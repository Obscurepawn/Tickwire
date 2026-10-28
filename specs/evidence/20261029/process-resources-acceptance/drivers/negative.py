import datetime,hashlib,json,os,subprocess,time
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');bins=Path('/tmp/tw-process-resources-bins-1');root=Path('/tmp/tw-res-negative-1');prefix='process-resources-negative'
verify=['python3',str(tmp/'process-resources-source-manifest.py'),'--verify']
def check_source(stage):
 with (tmp/f'{prefix}-source-{stage}.log').open('w') as f:subprocess.run(verify,check=True,stdout=f)
check_source('before');started=datetime.datetime.now(datetime.timezone.utc).isoformat();start=time.monotonic()
manifest=json.loads((tmp/'process-resources-binaries.json').read_text());before={r['name']:hashlib.sha256(Path(r['deployed_path']).read_bytes()).hexdigest() for r in manifest['binaries']};assert before=={r['name']:r['deployed_sha256'] for r in manifest['binaries']}
gen=[str(bins/'tickwire-lab'),'generate','--scenario','spot-fill','--out',str(root),'--bin-dir',str(bins)]
with (tmp/f'{prefix}-generate.log').open('w') as f:subprocess.run(gen,cwd=repo,check=True,stdout=f,stderr=subprocess.STDOUT)
scenario=json.loads((root/'scenario.json').read_text());config=Path(scenario['app_config']);app=json.loads(config.read_text());host=app['strategy']['config']
assert app['strategy']['mode']=='hosted'
cpu=min(os.sched_getaffinity(0));resources={}
for purpose in ['worker','source_replay','rollback']:
 p=root/f'fake-{purpose}';p.mkdir(mode=0o700);resources[purpose]={'cgroup':str(p),'cpus':[cpu],'numa_node':0,'memory_max':268435456}
host['resources']=resources;config.write_text(json.dumps(app,indent=2)+'\n');(tmp/f'{prefix}-resources.json').write_text(json.dumps(resources,indent=2)+'\n')
inspect=[str(bins/'tickwire-app'),'inspect','--config',str(config)]
with (tmp/f'{prefix}-inspect.log').open('w') as f:inspect_code=subprocess.call(inspect,cwd=repo,stdout=f,stderr=subprocess.STDOUT)
assert inspect_code==0
trace=tmp/f'{prefix}-trace';trace.mkdir();cmd=['strace','-ff','-s','256','-yy','-e','trace=process,openat,fstatfs,memfd_create','-o',str(trace/'trace'),str(bins/'tickwire-lab'),'run','--config',str(root/'scenario.json')]
with (tmp/f'{prefix}-run.log').open('w') as f:code=subprocess.call(cmd,cwd=repo,stdout=f,stderr=subprocess.STDOUT,timeout=120)
check_source('after');after={r['name']:hashlib.sha256(Path(r['deployed_path']).read_bytes()).hexdigest() for r in manifest['binaries']};assert before==after
traces={p.name:p.read_text() for p in trace.iterdir()};app_trace=[(n,t) for n,t in traces.items() if 'execve("'+str(bins/'tickwire-app')+'"' in t];assert len(app_trace)==1
name,events=app_trace[0];app_pid=int(name.split('.')[-1]);err=(root/'logs/exec.stderr.log').read_text();fixture=[json.loads(l) for l in (root/'fixture.jsonl').read_text().splitlines() if l]
worker_exec=[]
for name,t in traces.items():
 for l in t.splitlines():
  if ('execve(' in l or 'execveat(' in l) and any(n in l for n in ['tickwire-strategy-worker','tickwire-counting-worker','tickwire-counting-v1-worker']):worker_exec.append({'trace':name,'line':l})
orders=[v for v in fixture if v.get('method')=='POST' and '/order' in v.get('path','').lower()]
meta={'schema':1,'started_utc':started,'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-start,'root':str(root),'generate_command':gen,'inspect_command':inspect,'inspect_exit_code':inspect_code,'resources':resources,'run_command':cmd,'run_exit_code':code,'app_pid':app_pid,'app_fatal_log':err,'actual_app_trace':str(trace/f'trace.{app_pid}'),'worker_exec_events':worker_exec,'order_http_events':orders,'fixture_http_events':[v for v in fixture if v.get('event')=='http'],'md_admission_files':[p.name for p in (root/'md/admissions').iterdir()],'binary_hashes_before':before,'binary_hashes_after':after,'source_manifest_verified':True}
(tmp/f'{prefix}-metadata.json').write_text(json.dumps(meta,indent=2)+'\n');print(json.dumps(meta))
assert code!=0 and 'InvalidResources' in err and not worker_exec and not orders
assert str(root/'fake-worker') in events and 'fstatfs(' in events
assert not (root/'ARM').exists()
