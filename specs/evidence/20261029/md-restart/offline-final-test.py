import copy, hashlib, json, pathlib, shutil, subprocess, sys, tempfile
base=pathlib.Path('/home/obscure/Tickwire/specs/evidence/20261029')
out=pathlib.Path('/tmp/tickwire-progress-20261028')
report=json.loads((base/'md-restart.json').read_bytes())
root=pathlib.Path(tempfile.mkdtemp(prefix='tw-md-offline-final-',dir='/tmp'))
sha=lambda b:hashlib.sha256(b).hexdigest()
compact=lambda v:json.dumps(v,separators=(',',':'),ensure_ascii=False).encode()
report_hash=sha((base/'md-restart.json').read_bytes())
original={x['path']:sha((base/x['path']).read_bytes()) for x in report['artifacts']}
log=[];results=[]
def run(directory):
 p=subprocess.run([sys.executable,str(directory/'verify-md-restart.py'),str(directory/'md-restart.json')],capture_output=True,text=True)
 log.append({'directory':str(directory),'exit_code':p.returncode,'stdout':p.stdout.strip(),'stderr':p.stderr.strip()})
 return p,json.loads(p.stdout)
def fresh(name):
 d=root/name;d.mkdir()
 for x in report['artifacts']:
  target=d/x['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(base/x['path'],target)
 shutil.copy2(base/'md-restart.json',d/'md-restart.json')
 return d,copy.deepcopy(report)
def save_report(d,r): (d/'md-restart.json').write_text(json.dumps(r,indent=2)+'\n')
def sync(d,r,name):
 raw=(d/name).read_bytes();item=next(x for x in r['artifacts'] if x['path']==name);item.update(bytes=len(raw),sha256=sha(raw))
def json_change(d,r,name,fn):
 value=json.loads((d/name).read_bytes());fn(value);(d/name).write_bytes(compact(value));sync(d,r,name)
def rebound(d,r,fn,raw_change=None):
 prefix='md-restart/recovery/expanded';hp=prefix+'/HEAD';head=json.loads((d/hp).read_bytes());old=prefix+'/object-'+bytes(head['digest']).hex();value=json.loads((d/old).read_bytes());fn(value);raw=compact(value)
 if raw_change:raw=raw_change(raw)
 digest=sha(raw);new=prefix+'/object-'+digest;(d/new).write_bytes(raw);(d/old).unlink()
 item=next(x for x in r['artifacts'] if x['path']==old);item.update(path=new,bytes=len(raw),sha256=digest)
 head.update(digest=list(bytes.fromhex(digest)),bytes=len(raw));(d/hp).write_bytes(compact(head));sync(d,r,hp)
def negative(name,fn,expected):
 d,r=fresh(name);fn(d,r);save_report(d,r);p,value=run(d)
 assert p.returncode!=0 and value['status']=='failed' and expected in value['error'],(name,value)
 results.append({'case':name,'exit_code':p.returncode,'expected_failure':expected,'observed_failure':value['error']})
p,positive=run(base);assert p.returncode==0 and positive['status']=='verified'
d,r=fresh('portable-positive');p,portable=run(d);assert p.returncode==0 and portable==positive
negative('raw-artifact-corruption',lambda d,r:(d/'md-restart/cli-acceptance.log').write_bytes((d/'md-restart/cli-acceptance.log').read_bytes()+b'!'),'artifact length')
def missing(d,r):
 row=next(x for x in r['artifacts'] if x['path'].startswith('md-restart/recovery/expanded/object-'));(d/row['path']).unlink()
negative('missing-cas-object',missing,'No such file')
negative('rebound-dense-order',lambda d,r:rebound(d,r,lambda s:s['instrument_ids'].reverse()),'revision/dense prefix changed')
negative('rebound-lost-operation',lambda d,r:rebound(d,r,lambda s:s['operations'].pop(0)),'consumed history changed')
def owner(s):
 actual={bytes(f['owner_id']) for f in s['config']['feeds']}
 actual.update(bytes(e['owner_id']) for f in s['config']['feeds'] for e in f['events'])
 old=next(o for o in s['reserved_owners'] if bytes(o) not in actual);s['reserved_owners'].remove(old)
negative('rebound-lost-historical-owner',lambda d,r:rebound(d,r,owner),'consumed history changed')
negative('rebound-config-hash',lambda d,r:rebound(d,r,lambda s:s['config'].__setitem__('socket',s['config']['socket']+'-changed')),'Config hash')
negative('rebound-bank-descriptor',lambda d,r:rebound(d,r,lambda s:s['banks'][0].__setitem__(272,s['banks'][0][272]^1)),'bank descriptor SHA-256')
negative('rebound-noncanonical-json',lambda d,r:rebound(d,r,lambda s:None,lambda b:b+b'\n'),'noncanonical recovery JSON bytes')
negative('rebound-process-image',lambda d,r:json_change(d,r,'md-restart/cli-acceptance.json',lambda c:c['samples'][0].__setitem__('sha256','00'*32)),'actual process image binding')
negative('rebound-event-log-disagreement',lambda d,r:json_change(d,r,'md-restart/cli-acceptance.json',lambda c:c['events'][0].__setitem__('killed_pid',c['events'][0]['killed_pid']+1)),'metadata events differ')
def missing_phase(d,r):
 name='md-restart/cli-acceptance.json';value=json.loads((d/name).read_bytes());remove=next(e for e in value['events'] if e.get('phase')=='applied');value['events'].remove(remove);(d/name).write_bytes(compact(value));sync(d,r,name)
 name='md-restart/cli-acceptance.log';lines=[]
 for line in (d/name).read_bytes().splitlines():
  if line.startswith(b'{') and json.loads(line)==remove:continue
  lines.append(line)
 (d/name).write_bytes(b'\n'.join(lines)+b'\n');sync(d,r,name)
negative('rebound-missing-crash-stage',missing_phase,'four actual incomplete stages')
negative('unlisted-cas-object',lambda d,r:(d/('md-restart/recovery/expanded/object-'+'00'*32)).write_bytes(b'{}'),'unlisted CAS entry')
assert sha((base/'md-restart.json').read_bytes())==report_hash
assert original=={name:sha((base/name).read_bytes()) for name in original}
summary={'status':'verified','input_report_sha256':report_hash,'verifier_sha256':sha((base/'verify-md-restart.py').read_bytes()),'original_archive_unchanged':True,'positive':positive,'portable_copy_positive':portable,'negative_count':len(results),'negative_results':results,'copy_root':str(root),'limitations_review':{'overclaim_found':False,'scope':'Actual fixed Linux MD CLI process tests and archived content; no new network run in this audit, launch authority, production performance, live Binance, physical power-loss, or transparent old-consumer reconnect claim. Quota preservation was tested online; quota store is not independently replayed by this offline verifier.'}}
(out/'md-restart-offline-final.log').write_text('\n'.join(json.dumps(v,ensure_ascii=False) for v in log)+'\n'+json.dumps(summary,ensure_ascii=False)+'\n')
(out/'md-restart-offline-final.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False)+'\n')
print(json.dumps({'status':summary['status'],'artifacts':positive['artifacts'],'samples':positive['actual_process_samples'],'negative_cases':len(results),'original_unchanged':True,'log':str(out/'md-restart-offline-final.log'),'json':str(out/'md-restart-offline-final.json')}))
