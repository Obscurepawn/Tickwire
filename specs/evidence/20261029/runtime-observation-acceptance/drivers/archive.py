import datetime,hashlib,json,re,shutil,tempfile
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');base=repo/'specs/evidence/20261029';destination=base/'runtime-observation-acceptance'
assert not destination.exists(),destination
staging=tempfile.TemporaryDirectory(prefix='.runtime-observation-',dir=base);bundle=Path(staging.name)
def load(p):return json.loads(p.read_text())
artifacts={}
def put(src,name):
 assert not Path(name).is_absolute() and '..' not in Path(name).parts
 dst=bundle/name;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(src,dst);raw=dst.read_bytes();ref=(destination/name).relative_to(base).as_posix();artifacts[ref]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'original_path':str(src)};return ref
source=load(base/'runtime-observation-source-sha256.json');bm=load(tmp/'runtime-observation-binaries.json');build=load(tmp/'runtime-observation-build-meta.json')
report={'schema':1,'assembled_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'本机 Linux 固定 release ELF 的持续运行查询、App 持久观测入口及交接/重建兼容验收；无全局配置 Applied、生产资格或性能证明。','source_binding':{'manifest':'runtime-observation-source-sha256.json','manifest_sha256':hashlib.sha256((base/'runtime-observation-source-sha256.json').read_bytes()).hexdigest(),'file_count':source['file_count'],'git_head':source['git_head'],'verification_logs':[]},'build':{**build,'environment':{'CARGO_TARGET_DIR':str(tmp/'target')},'log':put(tmp/'runtime-observation-release-build.log','build/release-build.log'),'binary_manifest':put(tmp/'runtime-observation-binaries.json','build/binaries.json'),'deployment_strip':'--strip-debug','binary_count':8},'scenarios':[]}
report['final_rust_gate']={'metadata':put(tmp/'runtime-observation-check-rust-meta.json','build/check-rust-meta.json'),'log':put(tmp/'runtime-observation-check-rust.log','build/check-rust.log')}
gate_counts=[tuple(map(int,m)) for m in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', (tmp/'runtime-observation-check-rust.log').read_text())]
report['final_rust_gate']['reported_batches']={'count':len(gate_counts),'passed':sum(x[0] for x in gate_counts),'failed':sum(x[1] for x in gate_counts),'ignored':sum(x[2] for x in gate_counts),'scope':'sum of reported Rust test batches, including nested test subprocess output; not a unique test count'}
for stage in ['before-build','after-build']:
 report['source_binding']['verification_logs'].append(put(tmp/f'runtime-observation-source-{stage}.log',f'build/source-{stage}.log'))
for case in ['handover','rebuild']:
 root=Path(f'/tmp/tw-obs-{case}-1');meta=load(tmp/f'runtime-observation-{case}-metadata.json');result=meta['result'];proof=result['preflight'];files={}
 for name in ['run-result.json','account-report.json','handover/preflight-verified.json','handover/preflight-final-verified.json','fixture.jsonl','export/records.jsonl','logs/exec.stderr.log','logs/coordinator.stdout.log','logs/coordinator.stderr.log','handover/keys.json']+['export/baseline.cbor','export/input/segment-00000000000000000001.wal']+[x.relative_to(root).as_posix() for x in sorted((root/'export/records').glob('*.cbor'))]+(['handover/captured.json'] if case=='rebuild' else []):files[name]=put(root/name,f'{case}/{name}')
 for name in ['metadata.json','live-images.json','generate.log','run.log','source-before.log','source-after.log']:files[name]=put(tmp/f'runtime-observation-{case}-{name}',f'{case}/{name}')
 receipts=[]
 for p in sorted((root/'handover/receipts').glob('*.json')):
  signed=load(p);payload=bytes(signed['payload']);body=json.loads(payload);phase=body.get('phase','prepared');domain='tickwire.lifecycle.applied.v1' if phase!='prepared' else f"tickwire.lifecycle.{'replay' if case=='handover' else 'rebuild'}-prepared.v1"
  receipts.append({'phase':phase,'file':put(p,f'{case}/handover/receipts/{p.name}'),'signature_domain':domain,'signer_public_key':'exec','payload_sha256':hashlib.sha256(payload).hexdigest(),'handover_sequence':body.get('handover_sequence'),'issued_ns':body.get('issued_ns'),'valid_until_ns':body.get('valid_until_ns')})
 lines=[json.loads(l) for l in (root/'logs/exec.stderr.log').read_text().splitlines() if l.startswith('{')];exits=[v for v in lines if v.get('event')=='strategy_retired'];assert len(exits)==1
 row={'scenario':meta['scenario'],'original_root':str(root),'started_utc':meta['started_utc'],'completed_utc':meta['completed_utc'],'elapsed_seconds':(meta['completed_monotonic_ns']-meta['started_monotonic_ns'])/1e9,'generate_command':meta['generate_command'],'run_command':meta['run_command'],'exit_code':meta['run_exit_code'],'result_event':result['event'],'account_fill_verified':result['account_fill_verified'],'mirror_records_verified':result['mirror_records_verified'],'worker_report_position_verified':result['worker_report_position_verified'],'coordinator':load(root/'logs/coordinator.stdout.log'),'old_worker_exit':exits[0],'signed_receipts':receipts,'files':files,'memfd_sample':{'file':files['live-images.json'],'binary_name':'tickwire-counting-worker','matched_deployed_sha256':next(x['deployed_sha256'] for x in bm['binaries'] if x['name']=='tickwire-counting-worker')}}
 for key in ['target_pid','rollback_pid','target_final_sequence','action_remains_consumed']+(['capture_pid','target_install_sequence','prefix_verified','rollback_exited'] if case=='rebuild' else ['source_replay_pid','source_s_sequence','source_c_sequence']):row[key]=proof[key]
 report['scenarios'].append(row)
host=load(tmp/'runtime-observation-host-process-metadata.json');res=load(tmp/'runtime-observation-rebuild-evidence/result.json');files={}
for src,name in [(tmp/'runtime-observation-host-process-metadata.json','metadata.json'),(tmp/'runtime-observation-host-process.log','process.log'),(tmp/'runtime-observation-capture-evidence/CAPTURE_WINDOW.json','CAPTURE_WINDOW.json'),(tmp/'runtime-observation-capture-evidence/receipt.json','capture-receipt.json'),(tmp/'runtime-observation-rebuild-evidence/result.json','rebuild-result.json')]:files['host/'+name]=put(src,'host/'+name)
for stage in ['before','after']:report['source_binding']['verification_logs'].append(put(tmp/f'runtime-observation-host-source-{stage}.log',f'host/source-{stage}.log'))
report['host_process_tests']={**host,'test_count':6,'files':files,'rebuild_pid':res['pid'],'probe_pid':res['probe_pid'],'installed_sequence':res['C']['sequence'],'continued_sequence':res['D']['sequence'],'scope':'actual MD JOIN server/memfd publisher in parent thread plus independent workers; same PID capture/factory/install/continued input/independent rollback/v4 READY/normal detach, intent lane connected, new_commands=0'}
host_events=[json.loads(line) for line in (tmp/'runtime-observation-host-process.log').read_text().splitlines() if line.startswith('{')]
report['host_process_tests']['fresh_cases']=[x for x in host_events if x.get('case') in {'fresh_minimum_report_floor','fresh_timeout_preserves_worker','slow_report_control_reader'}]
assert len(report['host_process_tests']['fresh_cases'])==3
# The actual App observation run and closure audit are archived separately below.
app=load(tmp/'runtime-observation-app-acceptance.json');appfiles={}
for name,rawpath in app['artifacts'].items():
 appfiles[name]=put(Path(rawpath),f'app/{name}')
appfiles['input-history.wal']=put(Path(app['root'])/'export/input/segment-00000000000000000001.wal','app/input-history.wal')
appfiles['metadata.json']=put(tmp/'runtime-observation-app-acceptance.json','app/metadata.json')
report['app_observation']={**app,'files':appfiles}
report['app_observation'].pop('artifacts')
development=load(tmp/'runtime-observation-process-development.json')
report['development_regression']={'summary':put(tmp/'runtime-observation-process-development.json','development/summary.json'),'logs':{}}
for item in development['logs']:
 path=Path(item['path']);report['development_regression']['logs'][path.name]=put(path,'development/'+path.name)
for name in ['runtime-observation-process-binary.json','runtime-observation-process-build.log','runtime-observation-process-binary3.json','runtime-observation-process-build3.log','runtime-observation-process-clippy-final.log','runtime-observation-process-red.log','runtime-observation-mode2-red.log','runtime-observation-mode2-green.log','runtime-observation-outbox-order-red.log','runtime-observation-outbox-order-green.log']:
 report['development_regression']['logs'][name]=put(tmp/name,'development/'+name)
report['app_fixture_attempts']={'scope':'fixture-only failures, not implementation regressions or accepted runs','files':{}}
for pattern in ['runtime-observation-app-invalidpath*','runtime-observation-app-mirror-premature*']:
 for path in sorted(tmp.glob(pattern)):report['app_fixture_attempts']['files'][path.name]=put(path,'development/'+path.name)
report['lock_regression']={'files':{},'initial_gate':'failed: three transient inherited-lock reopen errors','first_delayed_replay':'failed: four remaining FileBlobStore inherited-lock errors','final_delayed_replay':'passed after the tested owner lifetime fixes','fault_injection':'exec entry delayed to hold a real fork-inherited CLOEXEC open-file description; not a performance result'}
for name in ['runtime-observation-lock-root-cause.json','runtime-observation-check-rust-attempt1-meta.json','runtime-observation-check-rust-attempt1.log','runtime-observation-journal-lock-red.log','runtime-observation-journal-lock-green.log','runtime-observation-app-lock-red.log','runtime-observation-app-lock-green.log','runtime-observation-lock-delayed-green.log','runtime-observation-lock-delayed-green2.log','runtime-observation-lock-delayed-green3.log','runtime-observation-replay-lock-red.log','runtime-observation-replay-lock-green.log','runtime-observation-blob-lock-red.log','runtime-observation-blob-lock-green.log']:
 report['lock_regression']['files'][name]=put(tmp/name,'development/'+name)
coverage=load(tmp/'runtime-observation-lock-coverage.json')
report['lock_regression']['coverage']=put(tmp/'runtime-observation-lock-coverage.json','development/lock-coverage.json')
for name in coverage['logs']:
 if name not in report['lock_regression']['files']:report['lock_regression']['files'][name]=put(tmp/name,'development/'+name)
report['offline_scope']='Archived byte hashes, signatures, exact account-prefix hashes and recorded identity/freshness/result bindings are independently checked. ELF and full recovery CAS are omitted; their complete semantic closures were checked online by Rust before archival, and are not reconstructed offline.'
for name in ['build.py','run.py','host.py','archive.py','source-manifest.py']:put(tmp/f'runtime-observation-{name}',f'drivers/{name}')
report['offline_negative_tests']={'script':put(tmp/'runtime-observation-verifier-negative-tests.py','validation/negative-tests.py'),'result':put(tmp/'runtime-observation-verifier-negative-tests.json','validation/negative-tests.json')}
put(tmp/'runtime-observation-acceptance-readme.md','README.md')
attributes=tmp/'runtime-observation-acceptance.gitattributes'
attributes.write_text('# Preserve captured Cargo stdout byte-for-byte.\nhost/process.log whitespace=-blank-at-eof\nbuild/check-rust.log whitespace=-blank-at-eof\ndevelopment/*.log whitespace=-blank-at-eof\n')
put(attributes,'.gitattributes')
verifier=base/'verify-runtime-observation-acceptance.py'
report['offline_verifier']={'file':verifier.name,'sha256':hashlib.sha256(verifier.read_bytes()).hexdigest()}
report['artifacts']=artifacts
assert app['status']=='verified' and app['passed'] and host['exit_code']==0 and build['exit_code']==0
assert 'test result: ok. 6 passed; 0 failed; 0 ignored;' in (tmp/'runtime-observation-host-process.log').read_text()
bundle.rename(destination);staging.cleanup()
(base/'runtime-observation-acceptance.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'files':len(artifacts),'path':str(base/'runtime-observation-acceptance.json')}))
