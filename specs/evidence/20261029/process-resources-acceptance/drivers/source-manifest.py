import argparse,datetime,hashlib,json,subprocess
from pathlib import Path
root=Path('/home/obscure/Tickwire')
p=argparse.ArgumentParser();p.add_argument('--verify',action='store_true');a=p.parse_args()
out=root/'specs/evidence/20261029/process-resources-source-sha256.json'
paths=set(subprocess.check_output(['rg','--files','--hidden','crates','specs/contracts','.cargo'],cwd=root).decode().splitlines())
paths={s for s in paths if Path(s).suffix.lower() not in {'.md','.svg','.png','.jpg'}}
paths.update(['Cargo.toml','Cargo.lock','rust-toolchain.toml','scripts/protoc-raft','scripts/check-rust.sh'])
files=[]
for path in sorted(paths):
 raw=(root/path).read_bytes();files.append({'path':path,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
if a.verify:
 old=json.loads(out.read_text());assert old['files']==files,'Build inputs changed after source freeze'
 print(f'UNCHANGED: {len(files)} build inputs')
else:
 data={'schema':1,'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'root':str(root),'git_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root).decode().strip(),'scope':'Workspace, lockfile, toolchain, .cargo, all non-document crate and contract files, protoc wrapper and full Rust gate script; ignored build products excluded by rg','file_count':len(files),'files':files}
 out.write_text(json.dumps(data,indent=2)+'\n');print(f'FROZEN: {len(files)} build inputs')
