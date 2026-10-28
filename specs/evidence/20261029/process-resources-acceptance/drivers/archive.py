import datetime,hashlib,json,shutil
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');base=repo/'specs/evidence/20261029';bundle=base/'process-resources-acceptance';bundle.mkdir(exist_ok=True)
def load(p):return json.loads(p.read_text())
artifacts={}
def put(src,name):
 dst=bundle/name;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(src,dst);raw=dst.read_bytes();ref=dst.relative_to(base).as_posix();artifacts[ref]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'original_path':str(src)};return ref
source=load(base/'process-resources-source-sha256.json');bm=load(tmp/'process-resources-binaries.json');build=load(tmp/'process-resources-build-meta.json')
report={'schema':1,'assembled_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'本机 Linux 固定 release ELF 的实际进程兼容验收及 App 普通目录资源拒绝；无 cgroup 委派正向资格、生产资格、性能或全局配置 Applied 证明。','source_binding':{'manifest':'process-resources-source-sha256.json','manifest_sha256':hashlib.sha256((base/'process-resources-source-sha256.json').read_bytes()).hexdigest(),'file_count':source['file_count'],'git_head':source['git_head'],'verification_logs':[]},'build':{**build,'environment':{'CARGO_TARGET_DIR':str(tmp/'target')},'log':put(tmp/'process-resources-release-build.log','build/release-build.log'),'binary_manifest':put(tmp/'process-resources-binaries.json','build/binaries.json'),'deployment_strip':'--strip-debug','binary_count':8},'scenarios':[]}
for stage in ['before-build','after-build']:
 report['source_binding']['verification_logs'].append(put(tmp/f'process-resources-source-{stage}.log',f'build/source-{stage}.log'))
for case in ['handover','rebuild']:
 root=Path(f'/tmp/tw-res-{case}-1');meta=load(tmp/f'process-resources-{case}-metadata.json');result=meta['result'];proof=result['preflight'];files={}
 for name in ['run-result.json','account-report.json','handover/preflight-verified.json','handover/preflight-final-verified.json','fixture.jsonl','export/records.jsonl','logs/exec.stderr.log','logs/coordinator.stdout.log','logs/coordinator.stderr.log','handover/keys.json']+(['handover/captured.json'] if case=='rebuild' else []):files[name]=put(root/name,f'{case}/{name}')
 for name in ['metadata.json','live-images.json','generate.log','run.log','source-before.log','source-after.log']:files[name]=put(tmp/f'process-resources-{case}-{name}',f'{case}/{name}')
 receipts=[]
 for p in sorted((root/'handover/receipts').glob('*.json')):
  signed=load(p);payload=bytes(signed['payload']);body=json.loads(payload);phase=body.get('phase','prepared');domain='tickwire.lifecycle.applied.v1' if phase!='prepared' else f"tickwire.lifecycle.{'replay' if case=='handover' else 'rebuild'}-prepared.v1"
  receipts.append({'phase':phase,'file':put(p,f'{case}/handover/receipts/{p.name}'),'signature_domain':domain,'signer_public_key':'exec','payload_sha256':hashlib.sha256(payload).hexdigest(),'handover_sequence':body.get('handover_sequence'),'issued_ns':body.get('issued_ns'),'valid_until_ns':body.get('valid_until_ns')})
 lines=[json.loads(l) for l in (root/'logs/exec.stderr.log').read_text().splitlines() if l.startswith('{')];exits=[v for v in lines if v.get('event')=='strategy_retired'];assert len(exits)==1
 row={'scenario':meta['scenario'],'original_root':str(root),'started_utc':meta['started_utc'],'completed_utc':meta['completed_utc'],'elapsed_seconds':(meta['completed_monotonic_ns']-meta['started_monotonic_ns'])/1e9,'generate_command':meta['generate_command'],'run_command':meta['run_command'],'exit_code':meta['run_exit_code'],'result_event':result['event'],'account_fill_verified':result['account_fill_verified'],'mirror_records_verified':result['mirror_records_verified'],'worker_report_position_verified':result['worker_report_position_verified'],'coordinator':load(root/'logs/coordinator.stdout.log'),'old_worker_exit':exits[0],'signed_receipts':receipts,'files':files,'memfd_sample':{'file':files['live-images.json'],'binary_name':'tickwire-counting-worker','matched_deployed_sha256':next(x['deployed_sha256'] for x in bm['binaries'] if x['name']=='tickwire-counting-worker')}}
 for key in ['target_pid','rollback_pid','target_final_sequence','action_remains_consumed']+(['capture_pid','target_install_sequence','prefix_verified','rollback_exited'] if case=='rebuild' else ['source_replay_pid','source_s_sequence','source_c_sequence']):row[key]=proof[key]
 report['scenarios'].append(row)
host=load(tmp/'process-resources-host-process-metadata.json');res=load(tmp/'process-resources-rebuild-evidence/result.json');files={}
for src,name in [(tmp/'process-resources-host-process-metadata.json','metadata.json'),(tmp/'process-resources-host-process.log','process.log'),(tmp/'process-resources-capture-evidence/CAPTURE_WINDOW.json','CAPTURE_WINDOW.json'),(tmp/'process-resources-capture-evidence/receipt.json','capture-receipt.json'),(tmp/'process-resources-rebuild-evidence/result.json','rebuild-result.json')]:files['host/'+name]=put(src,'host/'+name)
for stage in ['before','after']:report['source_binding']['verification_logs'].append(put(tmp/f'process-resources-host-source-{stage}.log',f'host/source-{stage}.log'))
report['host_process_tests']={**host,'files':files,'rebuild_pid':res['pid'],'probe_pid':res['probe_pid'],'installed_sequence':res['C']['sequence'],'continued_sequence':res['D']['sequence'],'scope':'actual MD JOIN server/memfd publisher in parent thread plus independent workers; same PID capture/factory/install/continued input/independent rollback/v4 READY/normal detach, intent lane connected, new_commands=0'}
negative=load(tmp/'process-resources-negative-metadata.json');nfiles={}
for name in ['metadata.json','resources.json','generate.log','inspect.log','run.log','source-before.log','source-after.log']:nfiles[name]=put(tmp/f'process-resources-negative-{name}',f'negative/{name}')
for name in ['logs/exec.stderr.log','logs/exec.stdout.log','logs/md.stderr.log','fixture.jsonl']:nfiles[name]=put(Path(negative['root'])/name,f'negative/{name}')
for p in sorted((tmp/'process-resources-negative-trace').iterdir()):nfiles[p.name]=put(p,f'negative/trace/{p.name}')
report['app_resource_negative']={**negative,'files':nfiles,'scope':'valid strict HostConfig.resources, three distinct ordinary directories; real App reaches resource acquisition and rejects filesystem before worker exec; real fixture receives no order POST','positive_cgroup_test':'not run: no writable delegated cgroup v2 leaf; no sudo or bypass attempted'}
for name in ['build.py','run.py','host.py','negative.py','archive.py','source-manifest.py']:put(tmp/f'process-resources-{name}',f'drivers/{name}')
attributes=tmp/'process-resources-acceptance.gitattributes'
attributes.write_text('# Preserve captured Cargo stdout byte-for-byte.\nhost/process.log whitespace=-blank-at-eof\n')
put(attributes,'.gitattributes')
report['artifacts']=artifacts
(base/'process-resources-acceptance.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'files':len(artifacts),'path':str(base/'process-resources-acceptance.json')}))
