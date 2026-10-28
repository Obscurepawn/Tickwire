import datetime,hashlib,json,platform,shutil,subprocess
from pathlib import Path
repo=Path('/home/obscure/Tickwire');tmp=Path('/tmp/tickwire-progress-20261028');e=repo/'specs/evidence/20261029';a=e/'strategy-inputs-artifacts';a.mkdir(exist_ok=True)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def copy(src,rel):
 dest=a/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dest);return str(dest.relative_to(e))
def j(path):return json.loads(path.read_text())
logs={
 'strategy-inputs-check-rust.log':'check-rust.log','strategy-inputs-check-rust-meta.json':'check-rust-meta.json',
 'strategy-inputs-contracts.log':'contracts.log','strategy-inputs-dag.json':'dependency-dag.json','strategy-inputs-docs-final.log':'documentation.log','strategy-inputs-style.json':'writing-style.json',
 'strategy-inputs-release-build.log':'release-build.log','strategy-inputs-build-meta.json':'build-meta.json','strategy-inputs-binaries.json':'binaries.json',
 'strategy-inputs-app-process.log':'app-process.log','strategy-inputs-app-process-meta.json':'app-process-meta.json',
 'strategy-inputs-parameter-compat.log':'parameter-compat.log','strategy-inputs-parameter-compat-meta.json':'parameter-compat-meta.json',
 'strategy-inputs-host-process.log':'host-process.log','strategy-inputs-host-process-meta.json':'host-process-meta.json',
 'strategy-inputs-compat-metadata.json':'compatibility/metadata.json','strategy-inputs-compat-live-images.json':'compatibility/live-images.json','strategy-inputs-compat-generate.log':'compatibility/generate.log','strategy-inputs-compat-run.log':'compatibility/run.log',
 'input-host-sdk-red.log':'development/sdk-red.log','input-host-sdk-final-tests.log':'development/sdk-green.log','input-host-sdk-minimal-check.log':'development/sdk-minimal.log',
 'input-host-red.log':'development/host-red.log','input-host-complete-tests.log':'development/host-green.log','input-host-clippy.log':'development/host-clippy.log',
 'input-app-red.log':'development/input-values-red.log','input-app-tests-final2.log':'development/input-values-green.log','input-app-lib-clippy.log':'development/input-values-clippy.log',
 'input-shared-query-red.log':'development/shared-query-red.log','input-startup-final.log':'development/shared-query-green.log',
 'input-process-red.log':'development/process-compile-failure.log','input-process-red2.log':'development/process-red.log',
 'input-process-green2.log':'development/process-canonical-fixture-failure.log','input-process-green3.log':'development/process-evidence-compile-failure.log',
 'input-process-final.log':'development/process-green.log','input-process-clippy-final.log':'development/process-clippy.log',
 'input-evidence-verifier-development.log':'development/offline-verifier.log','input-evidence-verifier-mutations.log':'development/offline-mutations.log',
}
for src,rel in logs.items():copy(tmp/src,rel)
for name in ['strategy-inputs-source-manifest.py','run-strategy-inputs-gate.py','strategy-inputs-build.py','run-strategy-inputs-process.py','run-strategy-inputs-parameter-compat.py','strategy-inputs-compat.py','run-strategy-inputs-host.py','archive-strategy-inputs.py']:
 copy(tmp/name,'reproduction/'+name)
rows=[json.loads(s) for s in (tmp/'strategy-inputs-app-process.log').read_text().splitlines() if s.startswith('{')]
fixtures=[]
for row in rows:
 if row.get('case')!='input_scope_consumed':continue
 root=Path(row['root']);label=root.name;fixture={'original_root':str(root)}
 for key,name in [('proof','inputs-proof.json'),('host_config','host-config.json'),('parameters','parameters.json'),('schema','parameter-schema.json'),('projection','input-projection.json'),('bootstrap','bootstrap.json'),('baseline','baseline.raw'),('market_bank','market-bank.raw'),('report_bank','report-bank.raw'),('checkpoint','current-checkpoint.raw')]:
  if key=='projection':name=Path(j(root/'host-config.json')['projection_inputs']['object_file']).name
  if key=='schema' and not (root/name).is_file():continue
  fixture[key]=copy(root/name,'fixtures/'+label+'/'+name)
 checkpoint=root/'current-checkpoint.raw';assert checkpoint.stat().st_size==row['checkpoint']['bytes'] and sha(checkpoint)==bytes(row['checkpoint']['digest']).hex()
 objects=[]
 for obj in row['objects']:
  path=root/'input-proof-objects'/bytes(obj['digest']).hex();assert path.stat().st_size==obj['bytes'] and sha(path)==path.name
  if obj['bytes']>1024*1024:continue
  rel=copy(path,'fixtures/'+label+'/objects/'+path.name);objects.append({'object':obj,'path':rel})
  if path.read_bytes().startswith(b'TWSC0001'):
   assert 'original_join' not in fixture;fixture['original_join']=rel
 fixture['objects']=objects;assert 'original_join' in fixture
 descriptors=list((root/'recovery').glob('*.json'));assert len(descriptors)<=1
 if descriptors:
  descriptor=j(descriptors[0]);command=bytes(descriptor['command_digest']).hex()
  fixture['descriptor']=copy(descriptors[0],'fixtures/'+label+'/descriptor.json')
  fixture['command']=copy(root/'commands'/('object-'+command),'fixtures/'+label+'/command.bin')
 fixtures.append(fixture)
compat=Path('/tmp/tw-input-compat-1')
for name in ['run-result.json','account-report.json','fixture.jsonl','handover/preflight-verified.json','handover/preflight-final-verified.json','logs/exec.stdout.log','logs/exec.stderr.log']:
 path=compat/name
 if path.is_file():copy(path,'compatibility/'+name)
source=e/'strategy-inputs-source-sha256.json';meta=j(tmp/'strategy-inputs-app-process-meta.json');gate=j(tmp/'strategy-inputs-check-rust-meta.json')
summary={'schema':1,'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'baseline_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo).decode().strip(),
 'source_binding':{'manifest':source.name,'manifest_sha256':sha(source)},'build':{'binary_manifest':'strategy-inputs-artifacts/binaries.json'},
 'app_process':{'log':'strategy-inputs-artifacts/app-process.log','metadata':'strategy-inputs-artifacts/app-process-meta.json','exit_code':meta['exit_code'],'fixtures':fixtures,'external_image_samples':len(meta['live_images'])},
 'scope':{'gateway_submission':False,'global_applied':False,'input_path':'Actual App hosted::build, independent registered worker, actual MD JOIN, current merged checkpoint, exact account/market input inspection and first-intent gate','compatibility':'Six prior parameter consumer process tests, Host fresh-query regression, and separate complete Spot handover scenario omitting projection_inputs','formal_verification':'No new TLC or anchor rebasing; read-only historical correspondence audit'},
 'environment':{'platform':platform.platform(),'rustc':subprocess.check_output(['rustc','--version'],cwd=repo).decode().strip(),'linux_integration':'Native WSL2 Linux; Docker daemon unavailable','live_binance':False,'delegated_cgroup_positive':False},
 'checks':{'full_rust':{'metadata':'strategy-inputs-artifacts/check-rust-meta.json','exit_code':gate['exit_code']},'contracts_exit_code':0,'host_process':{'metadata':'strategy-inputs-artifacts/host-process-meta.json','exit_code':j(tmp/'strategy-inputs-host-process-meta.json')['exit_code']},'parameter_compat':{'metadata':'strategy-inputs-artifacts/parameter-compat-meta.json','exit_code':j(tmp/'strategy-inputs-parameter-compat-meta.json')['exit_code']},'compatibility':{'metadata':'strategy-inputs-artifacts/compatibility/metadata.json','exit_code':j(tmp/'strategy-inputs-compat-metadata.json')['run_exit_code']}},
 'development_findings':['Same-risk-domain different-account mapping was accepted by the pre-implementation stub; pure and actual-process behavioral regressions now reject it before child construction.','An expired startup gate could reopen if caller supplied earlier time; explicit Closed phase now latches all errors.','Two independent reviews found a deadline window after consumer validation; final fresh sampling now rechecks the original deadline before publishing either proof.','NormalizedMarket bank has zero product fields; explicit route and complete account catalog qualify product rather than manufacturing product evidence.','The Multi shadow fixture initially rewrote canonical registered parameters through sorted Value keys; the worker correctly rejected it. Typed fixture encoding corrected the test only.','Two test evidence compile errors are retained separately from behavioral Red evidence.'],
 'limits':['InputsProof and ParametersProof are historical facts only, not global Applied or trading authorization.','First profile supports one explicit account and one external topic; runtime reapproval, catalog artifact economics, risk budgets and allowed_symbols remain separate work.','Multi handover denial may shadow or pause members; it cannot activate a successor across the scoped gate.','MD full peer identity is approved/authenticated endpoint identity, not independent MD kernel image proof.','Offline verification checks archived immutable bytes and recorded assertions; it does not recreate a live process or independently authenticate past kernel observations.','ELF recovery objects over 1 MiB stay outside Git; full observed object lists and fixed binary hashes remain recorded.','No production latency, live Binance or full global configuration qualification is claimed.'],
 'artifacts':{}}
for path in sorted(a.rglob('*')):
 if path.is_file():summary['artifacts'][str(path.relative_to(e))]={'bytes':path.stat().st_size,'sha256':sha(path)}
(e/'strategy-inputs.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');print('Archived',len(summary['artifacts']),'files and',len(fixtures),'real input proofs')
