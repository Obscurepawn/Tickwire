"""Check configuration design examples and constraints, not a running config service."""
import collections, copy, json, pathlib, re, tomllib
from decimal import Decimal
import jsonschema

root=pathlib.Path(__file__).resolve().parents[1]
schema=json.loads((root/'specs/contracts/system-config.schema.json').read_text())
api=json.loads((root/'specs/contracts/control-api.openapi.json').read_text())
reference=tomllib.loads((root/'config/reference.toml').read_text())
template=tomllib.loads((root/'config/production.template.toml').read_text())
jsonschema.Draft202012Validator.check_schema(schema)
v=jsonschema.Draft202012Validator(schema,format_checker=jsonschema.FormatChecker())
v.validate(reference)
template_errors=list(v.iter_errors(template))
assert template_errors, 'Production input template must reject unresolved inputs'

def keyed(rows,key='id'):
    result={r[key]:r for r in rows}
    assert len(result)==len(rows),f'duplicate {key}'
    return result
def check(c):
    hosts=keyed(c['hosts']);procs=keyed(c['processes']);accounts=keyed(c['accounts']);venues=keyed(c['venues']);policies=keyed(c['risk_policies']);topics=keyed(c['ipc']['topics'])
    assert len({a['risk_domain_id'] for a in accounts.values()})==len(accounts)
    cpus=collections.defaultdict(set);memory=collections.Counter()
    for p in procs.values():
        h=hosts[p['host_id']]
        assert p['numa_node']<h['numa_nodes']
        assert all(0<=cpu<h['physical_cores'] for cpu in p['cpus'])
        if p['role']!='supervisor':
            assert not cpus[h['id']].intersection(p['cpus'])
            assert not set(h['housekeeping_cpus']).intersection(p['cpus'])
            cpus[h['id']].update(p['cpus'])
        if h['role']=='control-only':assert p['role']=='supervisor'
        memory[h['id']]+=p['memory_limit_bytes']
    for h in hosts.values():assert memory[h['id']]+h['shm_budget_bytes']<=h['memory_bytes']
    for a in accounts.values():
        venue=venues[a['venue_id']];policy=policies[a['risk_policy_id']]
        owner=procs[a['exec_process_id']];standby=procs[a['standby_exec_process_id']]
        assert owner['role']==standby['role']=='exec'
        assert owner['activation']=='active' and standby['activation']=='standby'
        assert hosts[owner['host_id']]['failure_domain']!=hosts[standby['host_id']]['failure_domain']
        assert standby['host_id']==c['replication']['receiver_host_id']
        assert all(procs[g]['role']=='guardian' for g in a['guardian_process_ids'])
        if venue['product']=='Spot':assert a['position_mode']==a['margin_mode']==a['asset_mode']=='NotApplicable'
        else:assert a['position_mode']!='NotApplicable' and a['margin_mode']!='NotApplicable' and a['asset_mode']!='NotApplicable'
        for o in a['instrument_overrides']:
            assert o['symbol'] in a['allowed_symbols']
            assert (o['expiry_epoch_ms']>0)==(o['contract_kind']=='Delivery')
    states=set();counts=collections.Counter();consumer_counts=collections.Counter();pairs=0
    for st in c['strategies']:
        p=procs[st['process_id']]
        assert p['role']=='strategy' and p['activation']==st['role'].lower()
        key=(st['logical_strategy_id'],st['role']);assert key not in states;states.add(key);counts[st['role']]+=1
        assert set(st['account_ids'])<=accounts.keys() and set(st['topic_ids'])<=topics.keys()
        budgets=keyed(st['risk_budgets'],'account_id');assert set(budgets)==set(st['account_ids'])
        for aid,b in budgets.items():
            pol=policies[accounts[aid]['risk_policy_id']]
            assert b['quote_asset']==pol['quote_asset']
            assert Decimal(b['max_order_quote'])<=Decimal(b['max_gross_quote'])
            assert Decimal(b['max_order_quote'])<=Decimal(pol['max_order_quote'])
        pairs+=len(st['account_ids'])
        consumer_counts.update(st['topic_ids'])
    for t in topics.values():
        assert procs[t['publisher_process_id']]['role']=='md'
        assert procs[t['standby_publisher_process_id']]['role']=='md'
        assert consumer_counts[t['id']]+len(accounts)<=t['consumer_limit']
    i=c['ipc']
    for n in ['market_capacity','intent_capacity','report_capacity']:assert i[n]&(i[n]-1)==0
    assert 0<i['ring_high_watermark_records']<i['ring_stop_claiming_records']<i['market_capacity']
    classes=keyed(i['market_arena_classes'],'class_id')
    assert sum(x['block_bytes']*x['block_count'] for x in classes.values())==i['market_arena_bytes_per_bank']
    assert all(x['block_bytes']%128==0 and x['payload_limit']<=x['block_bytes']-128 for x in classes.values())
    market_banks=len(topics)*i['market_banks']
    market_cursors=sum(t['consumer_limit']*i['market_banks'] for t in topics.values())
    market_bank_bytes=8192+i['market_capacity']*i['market_record_bytes']+i['market_arena_bytes_per_bank']
    intent_bank_bytes=8192+i['intent_capacity']*i['intent_record_bytes']
    report_bank_bytes=8192+i['report_capacity']*i['report_record_bytes']
    banks_and_cursors=market_banks*market_bank_bytes+market_cursors*8192+pairs*(i['intent_banks']*(intent_bank_bytes+8192)+i['report_banks']*(report_bank_bytes+8192))
    snapshot_owners=len(topics)+len(accounts)
    snapshots=snapshot_owners*i['snapshots_per_owner']*i['snapshot_max_bytes']
    projection_objects=i['config_projection_max_objects']*(4096+i['config_projection_max_bytes'])
    assert banks_and_cursors+snapshots+projection_objects+i['relay_budget_bytes']<=min(h['shm_budget_bytes'] for h in hosts.values() if h['role']=='trading')
    ctrl=c['control'];voters=keyed(ctrl['voters'],'node_id')
    assert len({hosts[x['host_id']]['failure_domain'] for x in voters.values()})==3
    assert i['resource_heartbeat_ms']<i['resource_session_ms']
    assert ctrl['exec_grant_renew_ms']<ctrl['exec_grant_lifetime_ms']<ctrl['strategy_grant_lifetime_ms']
    assert ctrl['strategy_progress_interval_ms']<ctrl['strategy_suspect_after_ms']
    assert ctrl['tick_ms']<=ctrl['heartbeat_ms']<ctrl['election_min_ms']<ctrl['election_max_ms']
    assert c['replication']['lag_warn_ms']<c['replication']['lag_freeze_ms']
    assert c['journal']['disk_warn_percent']<c['journal']['disk_freeze_percent']
    assert c['timeouts']['reconnect_min_ms']<c['timeouts']['reconnect_max_ms']
    return dict(strategy_instances=sum(counts.values()),active=counts['Active'],shadow=counts['Shadow'],strategy_domain_pairs=pairs,bank_and_cursor_bytes=banks_and_cursors,snapshot_bytes=snapshots,projection_object_bytes=projection_objects,total_with_relay_bytes=banks_and_cursors+snapshots+projection_objects+i['relay_budget_bytes'],relay_budget_bytes=i['relay_budget_bytes'])

result=check(reference)
dep=json.loads((root/'specs/contracts/deployment-reference-v1.json').read_text())
assert reference['identity']['deployment_profile']==dep['profile_id']
assert reference['journal']['commit_profile']==dep['default_storage_profile']
for k in ['active','shadow']:assert result[k]==dep['process_capacity'][k+'_strategies']
assert result['bank_and_cursor_bytes']==dep['shm']['all_bank_and_cursor_bytes']
assert result['snapshot_bytes']==dep['shm']['snapshot_budget_bytes']
assert reference['timeouts']['intent_ttl_us']==dep['load_contract']['intent_expiry_ms']*1000
assert reference['timeouts']['fencing_deadline_ms']==dep['timing']['fencing_deadline_ms']
assert reference['timeouts']['takeover_deadline_ms']==dep['timing']['takeover_deadline_ms']
assert reference['control']['operation_timeout_ms']==dep['timing']['config_operation_default_deadline_ms']
assert reference['qualification']['full_send_p99_us']==dep['slo']['normal']['complete_tick_to_socket_p99_us']
assert reference['qualification']['full_send_p999_us']==dep['slo']['normal']['complete_tick_to_socket_p999_us']
assert reference['qualification']['sync_send_p99_us']==dep['slo']['sync_replica_optional']['complete_tick_to_socket_p99_us']
assert reference['qualification']['sync_send_p999_us']==dep['slo']['sync_replica_optional']['complete_tick_to_socket_p999_us']
wire=json.loads((root/'specs/contracts/ipc-wire-v1.json').read_text())['profiles']['production_reference']
for i,row in enumerate(reference['ipc']['market_arena_classes']):
    for k,val in row.items():assert val==wire['market']['arena_classes'][i][k]

# Complete OpenAPI validation is owned by the repository validator --openapi.
for name,s in api['components']['schemas'].items():jsonschema.Draft202012Validator.check_schema(s)
refs=0
def walk(x):
    global refs
    if isinstance(x,dict):
        if '$ref' in x:
            r=x['$ref'];refs+=1
            if r.startswith('#/'):
                dest=api
                for part in r[2:].split('/'):dest=dest[part.replace('~1','/').replace('~0','~')]
            elif r=='system-config.schema.json':assert (root/'specs/contracts'/r).is_file()
            else:raise AssertionError(r)
        for y in x.values():walk(y)
    elif isinstance(x,list):
        for y in x:walk(y)
walk(api)
opids=set()
for path,verbs in api['paths'].items():
    placeholders=set(re.findall(r'{([^}]+)}',path))
    for method,op in verbs.items():
        assert op['operationId'] not in opids;opids.add(op['operationId'])
        resolved=[]
        for p in op.get('parameters',[]):
            if '$ref' in p:p=api['components']['parameters'][p['$ref'].split('/')[-1]]
            resolved.append(p)
        assert {p['name'] for p in resolved if p['in']=='path'}==placeholders
        if method=='post' and path not in ['/v1/config/validate','/v1/config/preview']:
            assert {'If-Match','Idempotency-Key'}<={p['name'] for p in resolved}

mutations=[('unknown-field',lambda c:c.update(unlisted=True)),('live-fixture-mode',lambda c:c['identity'].update(mode='live')),('record-size',lambda c:c['ipc'].update(intent_record_bytes=512)),('plaintext-credential-field',lambda c:c['accounts'][0].update(api_key='NOT-A-REAL-KEY'))]
for label,mutate in mutations:
    c=copy.deepcopy(reference);mutate(c);assert list(v.iter_errors(c)),label
for label,mutate in [('duplicate-process',lambda c:c['processes'].append(copy.deepcopy(c['processes'][0]))),('unsafe-cpu-sharing',lambda c:c['processes'][4].update(cpus=c['processes'][3]['cpus'])),('same-failure-domain',lambda c:c['hosts'][1].update(failure_domain=c['hosts'][0]['failure_domain'])),('non-power-ring',lambda c:c['ipc'].update(intent_capacity=4097)),('renew-after-expiry',lambda c:c['control'].update(exec_grant_renew_ms=200))]:
    c=copy.deepcopy(reference);mutate(c)
    try:check(c)
    except AssertionError:pass
    else:raise AssertionError('missed negative case '+label)

# Validate the actual API input variants and independent process-file schemas.
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
import hashlib
base='https://tickwire.invalid/contracts/control-api.openapi.json'
api_resource=dict(api, **{'$id':base})
registry=Registry().with_resource(base,Resource.from_contents(api_resource,default_specification=DRAFT202012)).with_resource(schema['$id'],Resource.from_contents(schema))
candidate_validator=jsonschema.Draft202012Validator({'$ref':base+'#/components/schemas/Candidate'},registry=registry)
etag='"cfg-1-'+('a'*64)+'"'
patch={'format':'scoped-patch','document':{'base_etag':etag,'changes':[{'op':'replace','selector':{'collection':'risk_policies','id':'spot-risk'},'path':'/max_order_quote','value':'500.00'}]}}
for candidate in [{'format':'json','document':reference},{'format':'toml','document':(root/'config/reference.toml').read_text()},patch]:candidate_validator.validate(candidate)
for bad_op in ['test','copy','move']:
    bad=copy.deepcopy(patch);bad['document']['changes'][0]['op']=bad_op;assert list(candidate_validator.iter_errors(bad))
bad=copy.deepcopy(patch);bad['document']['changes'][0]['path']='/allowed_symbols/0';assert list(candidate_validator.iter_errors(bad))
def component(name):return jsonschema.Draft202012Validator({'$ref':schema['$id']+'#/$defs/'+name},registry=registry,format_checker=jsonschema.FormatChecker())
def digest(value):return 'sha256:'+hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
strategy=reference['strategies'][0];process=next(p for p in reference['processes'] if p['id']==strategy['process_id'])
payload={'identity':reference['identity'],'process':process,'host':reference['hosts'][0],'strategy':strategy,'account_views':[{k:v for k,v in a.items() if k not in ['credential_ref','guardian_credential_ref','account_identity_ref']} for a in reference['accounts'] if a['id'] in strategy['account_ids']],'secret_bindings':[]}
global_hash=digest(reference);projection_hash=digest(payload)
projection_object={'projection_schema_version':1,'global_revision':'1','global_config_hash':global_hash,'projection_policy_revision':'1','target_process_id':process['id'],'projection_hash':projection_hash,'payload':payload}
component('ProjectionObject').validate(projection_object)
uid='00000000-0000-4000-8000-000000000001'
metadata={'revision':'1','global_config_hash':global_hash,'projection_hash':projection_hash,'projection_policy_revision':'1','projection_kind':'effective','target_process_id':process['id'],'instance_slot_id':uid,'operation_id':uid,'control_commit_ref':reference['qualification']['deployment_manifest_ref'],'process_boot_id':uid,'applied_event_seq':'1','journal_receipt_ref':reference['qualification']['deployment_manifest_ref'],'render_version':1}
projection={'projection_schema_version':1,'payload':payload,'_materialization':metadata}
component('ProcessProjection').validate(projection)
bad=copy.deepcopy(projection);bad['payload']['account_views'][0]['credential_ref']=reference['accounts'][0]['credential_ref'];assert list(component('ProcessProjection').iter_errors(bad))
bad=copy.deepcopy(projection);bad['payload']['secret_bindings']=[{'purpose':'execution','ref':reference['accounts'][0]['credential_ref']}];assert list(component('ProcessProjection').iter_errors(bad))
locator={'locator_version':1,'generation':'1','cluster_id':reference['identity']['cluster_id'],'active_root':reference['local_config']['root_dir'],'migration_id':uid,'manifest_hash':global_hash,'previous_locator_hash':'sha256:'+('0'*64)}
locator['checksum']=digest(locator);component('ConfigLocator').validate(locator)
bad=copy.deepcopy(locator);bad['active_root']='/var/lib/..';assert list(component('ConfigLocator').iter_errors(bad))
target_validator=jsonschema.Draft202012Validator({'$ref':base+'#/components/schemas/Target'},registry=registry)
target={'host_id':'host-a','process_id':process['id'],'expected_boot_id':uid,'account_ids':strategy['account_ids'],'action':'RollingRestart','projection_hash':projection_hash,'projection_policy_revision':'1','projection_object_hash':digest(projection_object),'planned_successor':{'replacement_slot_id':uid,'host_id':'host-a','generation':'2','artifact_hash':global_hash,'bootstrap_nonce':uid}}
target_validator.validate(target)
bad=copy.deepcopy(target);bad.pop('planned_successor');assert list(target_validator.iter_errors(bad))
print(json.dumps({'reference_schema_errors':0,'template_expected_errors':len(template_errors),'cross_field_checks':'passed','deployment_and_ipc_consistency':'passed','openapi_complete_check':'run_repository_validator_with_openapi','openapi_operations':len(opids),'openapi_refs_resolved':refs,'negative_cases_passed':len(mutations)+13,'candidate_variants_passed':3,'projection_and_locator_examples':'passed','capacity':result},indent=2))
