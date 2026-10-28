import datetime,hashlib,json,platform,re,shutil,subprocess
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');e=repo/'specs/evidence/20261029';a=e/'strategy-parameters-artifacts';a.mkdir(exist_ok=True)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def copy(src,rel):
 dest=a/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dest);return str(dest.relative_to(e))
def j(path):return json.loads(path.read_text())
logs={
 'strategy-parameters-check-rust.log':'check-rust.log','strategy-parameters-check-rust-meta.json':'check-rust-meta.json',
 'strategy-parameters-contracts.log':'contracts.log','strategy-parameters-dag.json':'dependency-dag.json','strategy-parameters-docs-final.log':'documentation.log','strategy-parameters-style.json':'writing-style.json',
 'strategy-parameters-release-build.log':'release-build.log','strategy-parameters-build-meta.json':'build-meta.json','strategy-parameters-binaries.json':'binaries.json',
 'strategy-parameters-app-process.log':'app-process.log','strategy-parameters-app-process-meta.json':'app-process-meta.json',
 'strategy-parameters-host-process.log':'host-process.log','strategy-parameters-host-process-meta.json':'host-process-meta.json',
 'strategy-parameters-compat-metadata.json':'compatibility/metadata.json','strategy-parameters-compat-live-images.json':'compatibility/live-images.json','strategy-parameters-compat-generate.log':'compatibility/generate.log','strategy-parameters-compat-run.log':'compatibility/run.log',
 'parameter-values-red.log':'development/values-red.log','parameter-values-green.log':'development/values-green.log','parameter-host-red.log':'development/host-red.log','parameter-host-tests-final.log':'development/host-green.log','parameter-host-process.log':'development/host-domain-failure.log','parameter-host-process-green.log':'development/host-domain-green.log',
 'parameter-app-red2.log':'development/app-red.log','parameter-poll-budget-red.log':'development/poll-cadence-red.log','parameter-poll-budget-green.log':'development/poll-cadence-green.log',
 'projection-parameters-runtime-red.log':'development/app-process-red.log','projection-parameters-runtime-green1.log':'development/depth-fixture-failure.log','projection-parameters-runtime-cadence.log':'development/join-race-failure.log',
 'projection-parameters-join-clock-red.log':'development/join-clock-red.log','projection-parameters-join-clock-green2.log':'development/join-clock-green.log','projection-parameters-join-clock-green1.log':'development/join-snapshot-fixture-failure.log','projection-parameters-join-clock-green-final.log':'development/join-fd-fixture-failure.log',
}
for src,rel in logs.items():copy(tmp/src,rel)
for name in ['strategy-parameters-source-manifest.py','run-strategy-parameters-gate.py','strategy-parameters-build.py','run-strategy-parameters-process.py','strategy-parameters-compat.py','run-strategy-parameters-host.py','archive-strategy-parameters.py']:
 copy(tmp/name,'reproduction/'+name)
rows=[json.loads(s) for s in (tmp/'strategy-parameters-app-process.log').read_text().splitlines() if s.startswith('{')]
fixtures=[]
for row in rows:
 if row.get('case')!='parameter_consumed':continue
 root=Path(row['root']);label=root.name
 fixture={'original_root':str(root)}
 for key,name in [('parameters','parameters.json'),('schema','parameter-schema.json'),('projection','projection.json'),('bootstrap','bootstrap.json')]:fixture[key]=copy(root/name,'fixtures/'+label+'/'+name)
 expected=bytes(row['checkpoint']['digest']).hex()
 candidates=list(root.rglob('object-'+expected));assert candidates,(root,expected)
 raw=candidates[0].read_bytes();assert len(raw)==row['checkpoint']['bytes'] and hashlib.sha256(raw).hexdigest()==expected
 fixture['checkpoint']=copy(candidates[0],'fixtures/'+label+'/checkpoint.json')
 descriptors=list((root/'recovery').glob('*.json'))
 assert len(descriptors)<=1
 if descriptors:
  descriptor=j(descriptors[0]);command=bytes(descriptor['command_digest']).hex()
  fixture['descriptor']=copy(descriptors[0],'fixtures/'+label+'/descriptor.json')
  fixture['command']=copy(root/'commands'/('object-'+command),'fixtures/'+label+'/command.bin')
 fixtures.append(fixture)
compat=Path('/tmp/tw-param-compat-1')
for name in ['run-result.json','account-report.json','fixture.jsonl','handover/preflight-verified.json','handover/preflight-final-verified.json','logs/exec.stdout.log','logs/exec.stderr.log']:
 path=compat/name
 if path.is_file():copy(path,'compatibility/'+name)
source=e/'strategy-parameters-source-sha256.json';meta=j(tmp/'strategy-parameters-app-process-meta.json');gate=j(tmp/'strategy-parameters-check-rust-meta.json')
summary={'schema':1,'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'baseline_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo).decode().strip(),
 'source_binding':{'manifest':source.name,'manifest_sha256':sha(source)},'build':{'binary_manifest':'strategy-parameters-artifacts/binaries.json'},
 'app_process':{'log':'strategy-parameters-artifacts/app-process.log','exit_code':meta['exit_code'],'fixtures':fixtures,'external_image_samples':{'consumed_workers':4,'legacy_workers':1,'note':'Five external kernel image samples cover four successful parameter consumers and one legacy compatibility worker; Host validates actual image identity for every successful proof.'}},
 'scope':{'gateway_submission':False,'global_applied':False,'parameter_path':'Actual App hosted::build, independent registered worker, parent-thread real MD, fresh current merged configuration and intent delivery gate','compatibility':'Separate complete Spot handover scenario omits projection_parameters','formal_verification':'No new TLC or anchor rebasing; read-only historical correspondence audit'},
 'environment':{'platform':platform.platform(),'rustc':subprocess.check_output(['rustc','--version'],cwd=repo).decode().strip(),'linux_integration':'Native WSL2 Linux; Docker daemon unavailable','live_binance':False,'delegated_cgroup_positive':False},
 'checks':{'full_rust':{'metadata':'strategy-parameters-artifacts/check-rust-meta.json','exit_code':gate['exit_code']},'contracts_exit_code':0,'host_process':{'metadata':'strategy-parameters-artifacts/host-process-meta.json','exit_code':j(tmp/'strategy-parameters-host-process-meta.json')['exit_code']},'compatibility':{'metadata':'strategy-parameters-artifacts/compatibility/metadata.json','exit_code':j(tmp/'strategy-parameters-compat-metadata.json')['run_exit_code']}},
 'development_findings':['Typed observation binds approved logical clock domain separately from actual kernel boot UUID; initial equality attempt was rejected and corrected.','Threshold fixture must publish actual depth deltas, since Quote alone does not alter strategy depth.','NotReady query attempts are limited to one cold advancement per10ms; original absolute deadline is checked first.','Actual App run exposed stale client poll time at WELCOME validation. A real SEQPACKET deterministic regression fails on the old state machine; one bounded deferred frame ensures next poll validates with a post-receive time.'],
 'limits':['ParametersProof is historical evidence only and is not a global configuration Applied receipt.','No production latency or full global configuration qualification is claimed.','Offline verifier checks archived bytes and recorded assertions; it does not recreate a live process or independently authenticate a past kernel observation.','Large ELF and complete recovery CAS stay outside Git; the real Host validates the complete closure during execution.'],
 'artifacts':{}}
for path in sorted(a.rglob('*')):
 if path.is_file():summary['artifacts'][str(path.relative_to(e))]={'bytes':path.stat().st_size,'sha256':sha(path)}
(e/'strategy-parameters.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');print('Archived',len(summary['artifacts']),'files and',len(fixtures),'real configurations')
