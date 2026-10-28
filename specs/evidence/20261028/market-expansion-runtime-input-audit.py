from pathlib import Path
import hashlib,json,struct
root=Path('/tmp/tw-exp-1028-1')
def crc32c(b):
 c=0xffffffff
 for v in b:
  c^=v
  for _ in range(8):c=(c>>1)^(0x82f63b78 if c&1 else 0)
 return (~c)&0xffffffff
def frames(path):
 b=path.read_bytes();i=0;previous=bytes(32)
 while i<len(b):
  assert b[i:i+8]==b'TWAL0001'
  n=struct.unpack_from('<I',b,i+12)[0];length=struct.unpack_from('<I',b,i+20)[0];f=b[i:i+n];p=f[136:136+length]
  assert hashlib.sha256(f[:-40]+f[-8:]).digest()==f[-40:-8]
  assert crc32c(p)==struct.unpack_from('<I',f,132)[0]
  h=bytearray(f[:136]);h[128:132]=bytes(4)
  assert crc32c(h)==struct.unpack_from('<I',f,128)[0]
  assert f[96:128]==previous;previous=f[-40:-8]
  assert f[-4:]==b'TEND' and struct.unpack_from('<I',f,n-8)[0]==n
  if struct.unpack_from('<H',f,16)[0]==2: yield struct.unpack_from('<Q',f,72)[0],p
  i+=n
 assert i==len(b)
def cbor(b):
 def get(i):
  a=b[i];major=a>>5;n=a&31;i+=1
  if n>=24:
   size={24:1,25:2,26:4,27:8}[n];n=int.from_bytes(b[i:i+size],'big');i+=size
  if major==0:return n,i
  if major==1:return -1-n,i
  if major in [2,3]:return b[i:i+n] if major==2 else b[i:i+n].decode(),i+n
  if major==4:
   v=[]
   for _ in range(n):x,i=get(i);v.append(x)
   return v,i
  raise ValueError((major,n))
 v,i=get(0);assert i==len(b);return v
rows=[]
for seq,p in frames(root/'export/input/segment-00000000000000000001.wal'):
 assert p[:8]==b'TWAI0001';r={'input_sequence':seq,'kind':p[8]}
 if p[8] in [4,10]:
  r.update(connection_epoch=struct.unpack_from('<Q',p,9)[0],gap_generation=struct.unpack_from('<Q',p,17)[0])
  if p[8]==4:r.update(reason=p[25],reason_name={4:'Clock'}[p[25]])
 if p[8]==8:r.update(snapshot_kind=p[9],connection_epoch=struct.unpack_from('<Q',p,10)[0],gap_generation=struct.unpack_from('<Q',p,18)[0],received_mono_ns=struct.unpack_from('<Q',p,34)[0],ledger=struct.unpack_from('<Q',p,66)[0],complete=bool(p[76]))
 if p[8]==3:
  rawlen=struct.unpack_from('<I',p,35)[0];tag=39+rawlen;r.update(connection_epoch=struct.unpack_from('<Q',p,9)[0],received_mono_ns=struct.unpack_from('<Q',p,27)[0],decision=p[tag])
  if p[tag]==2:r.update(reason=p[tag+1])
 rows.append(r)
reactivations=[]
for p in sorted((root/'export/records').glob('*.cbor')):
 a=cbor(p.read_bytes());assert a[0]=='tickwire.account-record'
 if a[4][0]!=10:continue
 v=a[4][1];assert len(v)==18
 r={'account_sequence':a[3],'prior_ledger_sequence':v[3],'input_stream':v[6].hex(),'private_connection_epoch':v[7],'gap_generation':v[8],'through_sequence':v[9],'query_coverage_digest':v[10].hex(),'evaluated_at':v[13],'valid_until':v[14],'writer_epoch':v[15],'execution_connection_epoch':v[16]}
 reactivations.append(r)
report={'schema':1,'root':str(root),'input_wal_sha256':hashlib.sha256((root/'export/input/segment-00000000000000000001.wal').read_bytes()).hexdigest(),'physical_frame_sha256_crc32c_chain_verified':True,'input_records':rows,'reactivations':reactivations}
out=Path('/tmp/tickwire-progress-20261028/runtime-expansion-input-audit.json');out.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'gap_records':[r for r in rows if r['kind']==4],'quarantine_records':[r for r in rows if r['kind']==3 and r['decision']==2],'reactivations':reactivations},indent=2))
comparison=[]
for name in ['tw-1028-r1','tw-exp-1028-1']:
 for f in sorted((Path('/tmp')/name/'export/records').glob('*.cbor')):
  a=cbor(f.read_bytes())
  if a[4][0]!=9:continue
  e=cbor(a[4][3]);identity=e[2];body=e[3]
  r={'run':name,'account_sequence':a[3],'input_sequence':a[4][2],'event_kind':body[0],'source_epoch':identity[7],'received_mono_ns':identity[11]}
  if body[0]==1:r.update(order_status=body[1][1],client_id=body[1][0][3],cumulative_quantity=body[1][5])
  comparison.append(r)
queries=[]
for seq,p in frames(root/'export/input/segment-00000000000000000001.wal'):
 if seq not in [8,10,11,17,18]:continue
 r={'input_sequence':seq,'kind':p[8]}
 if p[8]==6:r.update(response_sequence=int.from_bytes(p[9:17],'little'),effect_index=p[17])
 else:
  pos=9
  if p[8]==9:
   r.update(order_id=p[pos:pos+16].hex(),query_private_epoch=int.from_bytes(p[pos+16:pos+24],'little'),query_gap_generation=int.from_bytes(p[pos+24:pos+32],'little'));pos+=32
  n=int.from_bytes(p[pos:pos+4],'little');pos+=4+n
  r.update(request_epoch=int.from_bytes(p[pos:pos+8],'little'),received_mono_ns=int.from_bytes(p[pos+8:pos+16],'little'),http_status=int.from_bytes(p[pos+16:pos+18],'little'));pos+=18
  n=int.from_bytes(p[pos:pos+4],'little');raw=p[pos+4:pos+4+n];pos+=4+n
  body=json.loads(raw[raw.find(b'{'):]);r.update(response={k:body[k] for k in ['symbol','orderId','clientOrderId','status','origQty','executedQty','cummulativeQuoteQty'] if k in body},disposition=p[pos],event_count=p[pos+1])
 queries.append(r)
prior=[]
for seq,p in frames(Path('/tmp/tw-1028-b1/export/input/segment-00000000000000000001.wal')):
 if p[8]==4:prior.append({'input_sequence':seq,'epoch':int.from_bytes(p[9:17],'little'),'generation':int.from_bytes(p[17:25],'little'),'reason':p[25]})
report.update(ingress_event_comparison=comparison,order_query_evidence=queries,prior_custom_run_same_gap={'root':'/tmp/tw-1028-b1','gap_records':prior})
out.write_text(json.dumps(report,indent=2)+'\n')
