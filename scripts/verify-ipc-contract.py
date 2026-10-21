"""Verify binary design contracts and golden vectors, not a running IPC implementation."""
import json,hashlib,struct,pathlib,sys
R=pathlib.Path(sys.argv[1]).resolve() if len(sys.argv)>1 else pathlib.Path(__file__).resolve().parents[1];s=json.loads((R/'specs/contracts/ipc-wire-v1.json').read_text());vv=json.loads((R/'specs/contracts/ipc-vectors-v1.json').read_text());layouts={x['name']:x for x in s['layouts']}
formats={'u8':'B','i8':'b','u16':'H','i16':'h','u32':'I','i32':'i','u64':'Q','i64':'q','atomic_u64':'Q'}
checks=0
for name,l in layouts.items():
 assert l['size']%l['alignment']==0,name
 coverage=[0]*l['size'];names=set()
 for f in l['fields']:
  assert f['name'] not in names;names.add(f['name'])
  t=f['type'];n=f.get('count',1)
  unit=struct.calcsize('<'+formats[t]) if t in formats else (1 if t=='bytes' else layouts[t]['size'])
  alignment=unit if t in formats else (1 if t=='bytes' else layouts[t]['alignment'])
  assert f['size']==unit*n,(name,f)
  assert f['offset']%alignment==0,(name,f)
  assert 0<=f['offset']<=l['size']-f['size']
  if f.get('atomic'): assert f['offset']%128==0 and t=='atomic_u64'
  for i in range(f['offset'],f['offset']+f['size']):coverage[i]+=1
  checks+=1
 assert set(coverage)=={1},(name,set(coverage))
def decode(l,raw):
 assert len(raw)==layouts[l]['size'];d={}
 for f in layouts[l]['fields']:
  t=f['type'];n=f.get('count',1);r=raw[f['offset']:f['offset']+f['size']]
  if f.get('reserved'):assert not any(r),(l,f)
  if t=='bytes':d[f['name']]=r.hex()
  elif t in formats:
   v=list(struct.unpack('<'+formats[t]*n,r));d[f['name']]=v[0] if n==1 else v
  else:
   unit=layouts[t]['size'];v=[decode(t,r[i*unit:(i+1)*unit]) for i in range(n)];d[f['name']]=v[0] if n==1 else v
 return d
def encode(l,d):
 raw=bytearray(layouts[l]['size'])
 for f in layouts[l]['fields']:
  t=f['type'];n=f.get('count',1);v=d[f['name']]
  if t=='bytes':b=bytes.fromhex(v)
  elif t in formats:b=struct.pack('<'+formats[t]*n,*([v] if n==1 else v))
  else:b=b''.join(encode(t,x) for x in ([v] if n==1 else v))
  raw[f['offset']:f['offset']+f['size']]=b
 return bytes(raw)
def expected_part(actual,expected):
 for k,v in expected.items():
  if isinstance(v,dict):expected_part(actual[k],v)
  elif isinstance(v,list):
   for a,b in zip(actual[k],v):expected_part(a,b) if isinstance(b,dict) else None
  elif isinstance(v,str):assert actual[k].startswith(v) and set(actual[k][len(v):])<=set('0'),(k,v,actual[k])
  else:assert actual[k]==v,(k,v,actual[k])
vs={v['name']:v for v in vv['vectors']}
for v in vs.values():
 raw=bytes.fromhex(v['hex']);assert len(raw)==v['byte_length'];assert hashlib.sha256(raw).hexdigest()==v['sha256']
 if v['layout']=='ControlFrame':assert raw==b''.join(bytes.fromhex(vs[p]['hex']) for p in v['parts'])
 elif v['layout']=='CompositeFile':
  composed=bytearray(v['byte_length']);covered=set()
  for segment in v['segments']:
   part=bytes.fromhex(vs[segment['vector']]['hex']);start=segment['offset'];assert 0<=start<=len(composed)-len(part)
   positions=set(range(start,start+len(part)));assert not covered&positions;covered|=positions;composed[start:start+len(part)]=part
  assert bytes(composed)==raw
 else:
  d=decode(v['layout'],raw);expected_part(d,v['values']);assert encode(v['layout'],d)==raw
# Independent bit-at-a-time non-reflected CRC implementation (input/output reflected).
def reflect(x,n):return int(f'{x:0{n}b}'[::-1],2)
def crc(raw):
 c=0xffffffff
 for b in raw:
  c^=reflect(b,8)<<24
  for _ in range(8):c=((c<<1)^ (0x1edc6f41 if c & 0x80000000 else 0)) & 0xffffffff
 return reflect(c,32)^0xffffffff
assert crc(b'123456789')==0xe3069283
frame=bytes.fromhex(vs['freeze_complete_frame']['hex']);e=decode('ControlEnvelope',frame[:128]);assert e['frame_bytes']==len(frame);assert crc(frame[128:])==e['body_crc32c'];h=bytearray(frame[:128]);h[116:120]=b'\0'*4;assert crc(h)==e['header_crc32c']
for v in vs.values():
 if v['layout']=='ControlFrame':
  raw=bytes.fromhex(v['hex']);ev=decode('ControlEnvelope',raw[:128]);assert ev['frame_bytes']==len(raw);assert crc(raw[128:])==ev['body_crc32c'];head=bytearray(raw[:128]);head[116:120]=bytes(4);assert crc(head)==ev['header_crc32c']
# Independent selected field offsets and signed encoding checks.
a=bytes.fromhex(vs['new_limit_intent']['hex']);assert int.from_bytes(a[128+120:128+128],'little',signed=True)==6500000;assert int.from_bytes(a[128+384+24:128+384+32],'little')==101
b=bytes.fromhex(vs['fill_report']['hex']);assert b[128+256:128+264]==bytes.fromhex('bfffffffffffffff');assert b[128+316]==248
p=s['profiles']['production_reference'];m=p['market'];assert m['capacity']&(m['capacity']-1)==0;assert m['arena_offset']==8192+m['capacity']*m['record_bytes'];off=m['arena_offset'];arena=0
for c in m['arena_classes']:
 assert c['offset']==off and c['offset']%4096==0;assert c['length']==c['block_count']*c['block_bytes'];assert c['payload_limit']<=c['block_bytes']-128
 off+=c['length'];arena+=c['length']
assert arena==256*1024*1024==m['arena_bytes'];assert off==m['bank_bytes']
for q in ['intent','report']:assert p[q]['bank_bytes']==8192+p[q]['capacity']*p[q]['record_bytes']
d=p['deployment'];assert d['all_bank_and_cursor_bytes']==m['bank_bytes']*8+8192*320+(p['intent']['bank_bytes']+p['report']['bank_bytes'])*128+8192*256==3496542208
assert len({x['id'] for x in s['messages']})==len(s['messages']);assert len({x['id'] for x in s['control_opcodes']})==len(s['control_opcodes'])
for name,c in s['message_constraints'].items():
 declared={f['name'] for f in layouts[c['body_layout']]['fields'] if not f.get('reserved')};assert set(c['allowed_fields'])<=declared
 if 'value_validity_allowed_mask'in c:assert c['value_validity_required_mask']&~c['value_validity_allowed_mask']==0
# Note: relative-link validation for the ABI design doc lived here while the
# doc existed under docs/design/. That directory has been removed; Markdown
# link checking for specs/ and README/AGENTS now lives in
# verify-documentation.py.
import copy
import re
class SemanticError(Exception):pass
def fail(code):raise SemanticError(code)
def iszero(v):
 if isinstance(v,dict):return all(iszero(x) for x in v.values())
 if isinstance(v,list):return all(iszero(x) for x in v)
 if isinstance(v,str):return set(v)<=set('0')
 return v==0
def validate_message(d):
 mh=d['header'];b=d['body'];mid=mh['message_type'];m=next((x for x in s['messages'] if x['id']==mid),None)
 if not m:fail('UnknownMessage')
 c=s['message_constraints'][m['name']]
 for field,value in b.items():
  if field not in c['allowed_fields'] and not iszero(value):fail('ReservedNonzero')
 if 'value_validity_allowed_mask'in c:
  mask=b['value_validity']
  if mask&~c['value_validity_allowed_mask'] or mask&c['value_validity_required_mask']!=c['value_validity_required_mask']:fail('Malformed')
  for bit,field in enumerate(s['report_value_fields']):
   if not(mask&(1<<bit)) and b[field]!=0:fail('Malformed')
  if b['report_kind']!=c['report_kind']:fail('Malformed')
 if 'command_kind'in c and b['command_kind']!=c['command_kind']:fail('Malformed')
 if m['channel']=='StrategyIntent':
  if not 0<=b['trigger_count']<=8:fail('Bounds')
  if any(not iszero(x) for x in b['triggers'][b['trigger_count']:]):fail('ReservedNonzero')
  if mh['body_bytes']!=896 or mh['payload_kind'] or mh['feed_generation']:fail('Malformed')
 if m['name']=='Depth':
  if b['level_count']!=b['bid_count']+b['ask_count'] or mh['payload_length']!=b['level_count']*16:fail('Malformed')
  if mh['payload_kind']==1 and (b['level_count']>12 or any(not iszero(x) for x in b['inline_levels'][b['level_count']:])):fail('Malformed')
 return True
def validate_resource_shape(h,snapshot=None):
 kind=h['resource_kind'];cap=h['capacity'];record=h['record_bytes']
 if kind in [1,2,3]:
  if not 64<=cap<=1048576 or cap&(cap-1):fail('Bounds')
  if record!=({1:512,2:1024,3:512}[kind]):fail('Malformed')
 elif kind==4:
  if cap or record or h.get('resource_bytes')!=8192:fail('Malformed')
 elif kind==9:
  if record!=1 or not 1<=cap<=4194304:fail('Bounds')
 elif kind in [5,6,7,8]:
  if snapshot is None or cap!=snapshot['entry_count']:fail('Malformed')
  kinds={5:{1:16,5:512,6:64},6:{2:512},7:{3:256},8:{4:128}}
  if snapshot['snapshot_kind'] not in kinds[kind] or record!=kinds[kind][snapshot['snapshot_kind']]:fail('Malformed')
  if kind==8 and not 1<=cap<=4096:fail('Bounds')
 else:fail('InvalidEnum')
 return True
def validate_topic_bundle(v,context=None):
 raw=bytes.fromhex(v['hex']);rh=decode('ResourceHeader',raw[:4096]);sh=decode('SnapshotHeader',raw[4096:4352]);ctx=dict(v['semantic_context']);ctx.update(context or {})
 validate_resource_shape(rh,sh)
 assert rh['resource_bytes']==sh['object_bytes']==len(raw)<=67108864 and sh['entry_offset']==4352
 hdr=bytearray(raw[:4096]);hdr[272:304]=bytes(32);assert hashlib.sha256(hdr).hexdigest()==rh['descriptor_hash']
 entries_raw=raw[4352:4352+sh['entry_count']*128];assert hashlib.sha256(entries_raw).hexdigest()==sh['entries_hash']
 entries=[decode('TopicSnapshotEntry',entries_raw[i:i+128]) for i in range(0,len(entries_raw),128)]
 actual=[(x['instrument_id'],x['instrument_version']) for x in entries];expected=[(x['instrument_id'],x['instrument_version']) for x in ctx['expected_instruments']]
 if actual!=sorted(expected) or len({x[0] for x in actual})!=len(actual):fail('ResyncRequired')
 registryhash=hashlib.sha256(b''.join(struct.pack('<QQ',*x) for x in actual)).hexdigest()
 assert registryhash==sh['topic_instrument_set_hash']==ctx['expected_instrument_set_hash'];assert sh['state_version']==ctx['expected_instrument_set_version']
 start=((4352+len(entries_raw)+4095)//4096)*4096;assert not any(raw[4352+len(entries_raw):start]);members_total=0
 for e in entries:
  assert e['snapshot_offset']==start and e['snapshot_bytes']>=4096 and e['snapshot_bytes']%4096==0
  block=raw[start:start+e['snapshot_bytes']];assert len(block)==e['snapshot_bytes'] and hashlib.sha256(block).hexdigest()==e['snapshot_hash'];ch=decode('SnapshotHeader',block[:256])
  assert ch['snapshot_id']==e['snapshot_id'] and ch['object_bytes']==e['snapshot_bytes']
  assert ch['instrument_id']==e['instrument_id'] and ch['instrument_version']==e['instrument_version'];assert ch['snapshot_kind']==e['snapshot_kind'] and e['snapshot_kind'] in [1,5,6]
  assert ch['sequence']==e['snapshot_seq']==sh['sequence']==ctx['boundary_sequence'];assert e['last_instrument_event_seq']<=sh['sequence']
  assert ch['channel_id']==sh['channel_id'] and ch['feed_generation']==e['feed_generation']==sh['feed_generation'];assert ch['catalog_hash']==sh['catalog_hash']
  assert e['catalog_version']==rh['catalog_version'] and ch['entry_offset']==start+256
  size=ch['entry_count']*ch['entry_bytes'];payload=block[256:256+size];assert hashlib.sha256(payload).hexdigest()==ch['entries_hash']
  if ch['snapshot_kind']==1:
   assert ch['entry_bytes']==16 and ch['entry_count']==ch['bid_count']+ch['ask_count'];assert not any(block[256+size:])
  elif ch['snapshot_kind']==6:
   assert ch['entry_count']==2 and ch['entry_bytes']==64
   sections=[decode('SnapshotSection',payload[i:i+64]) for i in [0,64]];assert [x['section_kind'] for x in sections]==[1,2]
   section_cursor=start+384;state_types=[]
   for section in sections:
    assert section['offset']==section_cursor and section['length']==section['entry_count']*section['entry_bytes'];part=raw[section_cursor:section_cursor+section['length']];assert hashlib.sha256(part).hexdigest()==section['entries_hash']
    if section['section_kind']==1:
     assert section['entry_bytes']==512
     for row in range(section['entry_count']):
      record=decode('MarketRecord',part[row*512:(row+1)*512]);validate_message(record);assert record['header']['sequence']==row+1 and record['header']['instrument_id']==ch['instrument_id'] and record['header']['instrument_version']==ch['instrument_version'];assert record['header']['feed_generation']==ch['feed_generation'];state_types.append(record['header']['message_type'])
    else:assert section['entry_bytes']==16 and section['entry_count']==ch['bid_count']+ch['ask_count']
    section_cursor+=section['length']
   assert state_types==sorted(set(state_types)) and set(state_types)<={257,258,260,261} and 261 in state_types;assert not any(raw[section_cursor:start+e['snapshot_bytes']])
  else:
   assert ch['entry_bytes']==512;assert not any(block[256+size:])
  assert iszero(ch['topic_instrument_set_hash']) and ch['manifest_member_bytes']==0
  start+=e['snapshot_bytes'];members_total+=e['snapshot_bytes']
 assert start==len(raw) and members_total==sh['manifest_member_bytes']
 join=decode('JoinBody',bytes.fromhex(vs[ctx.get('join_vector_name','join_snapshot')]['hex']))
 assert join['snapshot_id']==sh['snapshot_id'] and join['snapshot_hash']==v['sha256'];assert join['topic_instrument_set_hash']==registryhash and join['topic_instrument_set_version']==sh['state_version'];assert join['snapshot_seq']==sh['sequence'] and join['join_seq']==sh['sequence']+1
 return True
def validate_typed_response(v):
 raw=bytes.fromhex(v['hex']);ev=decode('ControlEnvelope',raw[:128]);op=ev['opcode']
 if op==20:
  b=decode('QuiesceBody',raw[128:]);assert ev['flags']==10 and ev['status']==0 and b['evidence_kind']==1 and iszero(b['exit_evidence_id'])
 elif op==19:
  b=decode('SwitchBody',raw[128:]);assert iszero(b['snapshot_id']) and b['snapshot_seq']==0
  if ev['flags']==10:assert ev['status']==0 and b['switch_kind']==2
  else:assert ev['flags']==18 and ev['status']==32 and b['switch_kind']==3
 else:fail('UnknownMessage')
 return True
def validate_config_control(v,ctx,mutations=None):
 raw=bytes.fromhex(v['hex']);env=decode('ControlEnvelope',raw[:128]);b=decode('ConfigControlBody',raw[128:]);op=env['opcode'];flags=env['flags']
 for path,value in (mutations or {}).items():
  target=b if path.startswith('body.') else env;target[path.split('.')[-1]]=value
 if not(ctx['negotiated_features']&32):fail('UnsupportedFeature')
 if b['operation_id']!=env['operation_id'] or b['operation_revision']!=env['operation_revision']:fail('Identity')
 if b['actor_kind']!=ctx['authenticated_actor_kind'] or env['sender_instance']!=ctx['authenticated_sender_instance'] or b['issuer_boot_id']!=ctx['authenticated_issuer_boot_id']:fail('Permission')
 if env['fd_count'] or env['frame_bytes']!=896:fail('Malformed')
 for field,key in [('config_revision','committed_config_revision'),('base_revision','committed_base_revision'),('global_config_hash','committed_global_config_hash'),('control_commit_ref','committed_control_commit_ref'),('target_set_hash','committed_target_set_hash')]:
  if b[field]!=ctx[key]:fail('VersionConflict')
 if op==84 and b['config_status']==10:
  if b['actor_kind']!=1 or b['config_flags']!=1 or flags not in [10,32]:fail('Permission')
  for field in ['process_id_length','target_host_id_length','process_id','target_host_id','expected_instance_id','expected_boot_id','expected_host_boot_id','expected_exec_generation','instance_slot_id','projection_hash','projection_policy_revision','projection_object_hash','projection_object_bytes','projection_resource_id','runtime_receipt_hash','file_receipt_hash','journal_receipt_hash']:
   if not iszero(b[field]):fail('Malformed')
  if iszero(b['completion_receipt_hash']):fail('RecoveryEvidence')
  if not ctx.get('full_target_set_verified') or not ctx.get('completion_commit_verified'):fail('NotReady')
  return True
 for name,lenname in [('process_id','process_id_length'),('target_host_id','target_host_id_length')]:
  length=b[lenname];encoded=bytes.fromhex(b[name])
  if not 1<=length<=63 or any(encoded[length:]):fail('Malformed')
  try:value=encoded[:length].decode('ascii')
  except UnicodeDecodeError:fail('Malformed')
  if not re.fullmatch('[a-z][a-z0-9-]{0,62}',value):fail('Malformed')
  if name=='target_host_id' and value!=ctx['authenticated_host_id']:fail('Identity')
 for field,key in [('expected_instance_id','target_instance_id'),('expected_boot_id','target_boot_id'),('expected_host_boot_id','target_host_boot_id'),('expected_exec_generation','target_exec_generation')]:
  if b[field]!=ctx[key]:fail('Identity')
 if not 0<b['projection_object_bytes']<=4194304 or not b['projection_policy_revision']:fail('Bounds')
 if b['projection_transport']==1 and not iszero(b['projection_resource_id']):fail('Malformed')
 if b['projection_transport']==2 and iszero(b['projection_resource_id']):fail('MissingResource')
 if b['apply_class']==5:fail('UnsupportedOperation')
 if b['apply_class']==3 and (iszero(b['successor_binding_hash']) or not(b['config_flags']&2)):fail('Identity')
 if op==81:
  if b['actor_kind']!=2 or flags!=1 or b['config_status']!=3 or b['file_kind']!=1:fail('Permission')
  if env['target_instance']!=b['expected_instance_id']:fail('Identity')
  if not iszero(b['runtime_receipt_hash']) or iszero(b['file_receipt_hash']) or iszero(b['journal_receipt_hash']):fail('RecoveryEvidence')
  if ctx['current_runtime_revision']!=b['expected_runtime_revision'] or b['config_revision']<=ctx['current_runtime_revision']:fail('VersionConflict')
 elif op==82:
  if b['actor_kind']!=3 or b['config_status']!=5 or b['file_kind'] or not iszero(b['file_receipt_hash']):fail('Permission')
  if b['applied_event_seq']<1 or iszero(b['runtime_receipt_hash']) or iszero(b['journal_receipt_hash']):fail('RecoveryEvidence')
 elif op==83:
  if b['actor_kind']!=2 or flags!=1:fail('Permission')
  if b['config_status']==6:
   if b['file_kind']!=2 or b['applied_event_seq']<1 or not ctx.get('runtime_receipt_verified') or not ctx.get('file_receipt_verified'):fail('RecoveryEvidence')
  elif b['config_status']==2:
   if b['file_kind']!=1 or b['applied_event_seq'] or not iszero(b['runtime_receipt_hash']):fail('Malformed')
  else:fail('Malformed')
 else:fail('UnknownMessage')
 if not ctx.get('proof_objects_prevalidated'):fail('RecoveryEvidence')
 return True

# Exercise semantic validation for every fixed data-message vector.
for v in vs.values():
 if v['layout'] in ['MarketRecord','IntentRecord','ReportRecord','OwnershipRecord']:validate_message(decode(v['layout'],bytes.fromhex(v['hex'])))
semantic_checked=0
for case in vv['semantic_cases']:
 outcome='accept'
 try:
  if 'resource_kind'in case:
   validate_resource_shape(case,case if 'entry_count'in case else None)
  else:
   v=vs[case['vector']]
   if v['layout']=='CompositeFile':validate_topic_bundle(v,case.get('context'))
   elif v['layout']=='ControlFrame':
    opcode=decode('ControlEnvelope',bytes.fromhex(v['hex'])[:128])['opcode']
    if 80<=opcode<=84:validate_config_control(v,case['context'],case.get('mutations'))
    else:validate_typed_response(v)
   else:
    decoded=decode(v['layout'],bytes.fromhex(v['hex']))
    for path,value in case.get('mutations',{}).items():
     target=decoded;keys=path.split('.')
     for key in keys[:-1]:target=target[key]
     target[keys[-1]]=value
    if v['layout'] in ['MarketRecord','IntentRecord','ReportRecord','OwnershipRecord']:validate_message(decoded)
    elif v['layout']=='GrantBody':
     ctx=case['context'];lease=decode('ExecLeaseBody',bytes.fromhex(vs[ctx['exec_lease_vector']]['hex']));now=ctx['now_mono_ns']
     assert lease['deadline_mono_ns']-lease['issued_mono_ns']==s['profiles']['production_reference']['exec_lease_ms']*1000000
     assert lease['host_boot_id']==ctx['host_boot_id'] and lease['exec_generation']==ctx['exec_generation']==decoded['exec_generation']
     assert lease['issued_mono_ns']<=now<lease['deadline_mono_ns']==decoded['control_lease_deadline_mono_ns']
     assert now<ctx['resource_session_deadline_mono_ns']
     if not decoded['valid_from_mono_ns']<=now<decoded['valid_until_mono_ns']:fail('Deadline')
 except SemanticError as e:outcome=str(e)
 if 'expected_error'in case:assert outcome==case['expected_error'],(case['name'],outcome,case['expected_error'])
 else:assert outcome=='accept',(case['name'],outcome)
 semantic_checked+=1

baseline=vv.get('compatibility_baseline',{})
for name,sha in baseline.items():assert vs[name]['sha256']==sha
assert s['profiles']['production_reference']['config_control']['projection_resource_budget_bytes']==128*(4096+4194304)
assert d['all_committed_shm_bytes']==d['all_bank_and_cursor_bytes']+d['snapshot_budget_bytes']+d['projection_resource_budget_bytes']+d['relay_budget_bytes']==6449856512
assert d['shm_budget_remaining_bytes']==d['shm_resource_budget_bytes']-d['all_committed_shm_bytes']==2140078080
print(json.dumps({'result':'PASS','layouts':len(layouts),'fields_checked':checks,'vectors_roundtripped':len(vs),'data_message_ids':len(s['messages']),'control_opcodes':len(s['control_opcodes']),'crc32c_checks':1+2*sum(v['layout']=='ControlFrame' for v in vs.values()),'semantic_cases':semantic_checked,'unchanged_abi_vectors':len(baseline),'bank_and_cursor_bytes':d['all_bank_and_cursor_bytes'],'all_committed_shm_bytes':d['all_committed_shm_bytes'],'local_links':'PASS'},indent=2))
