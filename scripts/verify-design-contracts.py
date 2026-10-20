"""Verify design artifacts, not the trading implementation or its runtime behavior.

Run with Python 3.11+ and jsonschema installed. The optional --openapi check
requires openapi-spec-validator. No exchange access or credentials are used.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / 'specs/contracts'

def read(name):
    return json.loads((CONTRACTS / name).read_text())

def require(condition, message):
    if not condition:
        raise ValueError(message)

def run_helper(name):
    result = subprocess.run([sys.executable, str(ROOT/'scripts'/name)], cwd=ROOT,
                            capture_output=True, text=True)
    require(result.returncode == 0, f'{name}: {result.stdout}\n{result.stderr}')
    return json.loads(result.stdout)

def deployment_checks():
    p = read('deployment-reference-v1.json')
    a = read('ipc-wire-v1.json')['profiles']['production_reference']
    r = read('replication-profile-v1.json')
    require(p['default_storage_profile'] == r['default_profile'] == 'LocalDurable+AsyncReplica', 'default storage profile')
    require(not p['data_plane_consensus'] and r['groups']['risk_data_raft_group_count'] == 0, 'no per-order Raft')
    require(r['groups']['per_intent_control_group_round_trip_required'] is False, 'no per-intent control RTT')
    require(r['profiles'][r['default_profile']]['remote_ack_before_accepted_or_send'] is False, 'async send gate')
    require(r['topology']['execution_replicas'] == 2 and r['topology']['control_voters'] == 3, 'replica topology')
    hosts = {h['id']: h for h in p['hosts']}
    require(len(hosts)==3 and len({h['fault_domain'] for h in hosts.values()})==3, 'fault domains')
    require(not hosts['ctl-c']['holds_order_wal'], 'control-only voter is not order replica')
    require(sum(p['memory_unique_budget_bytes'].values()) == hosts['host-a']['memory_bytes'], 'unique memory budget')
    cpus = [v for values in p['cpu_reference'].values() for v in values]
    require(len(cpus)==len(set(cpus))==64 and set(cpus)==set(range(64)), 'reference CPU allocation')
    s=p['shm']; d=a['deployment']; c=p['process_capacity']
    market_banks=c['market_topics']*s['market_banks_per_topic']
    pairs=c['max_strategy_instances']*c['exec_risk_domains']
    spsc_banks=pairs*2*s['spsc_banks_per_direction']
    calculated=(market_banks*a['market']['bank_bytes']
                + market_banks*c['consumers_per_topic']*a['cursor_bytes']
                + pairs*s['spsc_banks_per_direction']*(a['intent']['bank_bytes']+a['report']['bank_bytes'])
                + spsc_banks*a['cursor_bytes'])
    require(calculated==s['all_bank_and_cursor_bytes']==d['all_bank_and_cursor_bytes'], 'SHM banks/cursors')
    require(s['snapshot_budget_bytes']==s['snapshot_owners']*s['snapshot_objects_per_owner']*s['snapshot_max_bytes'], 'snapshot count')
    require(s['base_allocated_bytes']==calculated+s['snapshot_budget_bytes'], 'base SHM')
    require(s['config_projection_budget_bytes']==s['config_projection_max_objects']*(s['config_projection_max_bytes']+s['config_projection_header_bytes']), 'projection reservation')
    require(s['committed_budget_bytes']==s['base_allocated_bytes']+s['relay_budget_bytes']+s['config_projection_budget_bytes'], 'relay and projection reservation')
    require(s['reserved_remaining_bytes']==s['resource_limit_bytes']-s['committed_budget_bytes']>0, 'SHM headroom')
    require(s['committed_budget_bytes']==d['all_committed_shm_bytes'] and s['config_projection_budget_bytes']==d['projection_resource_budget_bytes'], 'ABI projection and total SHM budget')
    for key in ['market_topics','exec_risk_domains','active_strategies','shadow_strategies','max_strategy_instances','strategy_domain_pairs']:
        require(c[key]==d[{'exec_risk_domains':'exec_domains'}.get(key,key)], f'ABI/deployment {key}')
    load=p['load_contract']
    trace=((load['total_market_events_per_second']*load['mean_serialized_market_bytes_for_disk_budget']
           +load['total_market_events_per_second']*load['max_consumer_instances_under_test']*load['ordered_input_reference_bytes']
           +load['synthetic_order_intents_per_second_total']*load['mean_order_journal_bytes_per_intent'])
           *load['disk_framing_overhead_numerator']//load['disk_framing_overhead_denominator'])
    checkpoint=(load['max_consumer_instances_under_test']*load['strategy_checkpoint_max_bytes']+load['strategy_checkpoint_interval_seconds']-1)//load['strategy_checkpoint_interval_seconds']
    require(trace==load['trace_nominal_bytes_per_second'] and checkpoint==load['checkpoint_worst_case_bytes_per_second'], 'disk rate calculation')
    require(trace+checkpoint==load['total_nominal_disk_bytes_per_second'], 'total disk rate')
    require((trace+checkpoint)*load['hot_retention_seconds']==load['hot_retention_nominal_bytes'], 'retention calculation')
    require(load['hot_retention_nominal_bytes'] < hosts['host-a']['usable_persistent_bytes']*80//100, 'retention fits warning boundary')
    require(p['storage_qualification']['minimum_sustained_write_bytes_per_second'] > trace*load['burst_multiplier']+checkpoint, 'burst disk qualification budget')
    require(p['slo']['normal']['complete_tick_to_socket_p99_us']==r['qualification_targets']['LocalDurable+AsyncReplica']['complete_tick_to_socket_p99_us']==300, 'default p99')
    require(p['slo']['normal']['complete_tick_to_socket_p999_us']==r['qualification_targets']['LocalDurable+AsyncReplica']['complete_tick_to_socket_p999_us']==1000, 'default p999')
    require(p['timing']['execution_authority_lease_ms']==r['execution_authority']['control_lease_lifetime_ms']==100, 'exec lease')
    require(p['timing']['execution_authority_renew_ms']==r['execution_authority']['control_lease_renew_interval_ms']==25, 'exec renew')
    require(p['timing']['ipc_resource_session_lease_ms']==a['resource_session_lease_ms']==3000, 'resource lease distinct')
    require(p['timing']['strategy_grant_lifetime_ms']==a['strategy_grant_default_ttl_ms']==60000, 'strategy grant')
    wal=r['wal']
    require(sum(x['bytes'] for x in wal['header_fields'])==wal['header_bytes']==136, 'WAL header width')
    require(sum(x['bytes'] for x in wal['trailer_fields'])==wal['trailer_bytes']==40, 'WAL trailer width')
    require('StorageQuarantined_rebuild_from_healthy_quorum' not in json.dumps(r), 'business log cannot be restored by control quorum')
    return {'profile':p['profile_id'],'bank_cursor_bytes':calculated,'shm_committed_bytes':s['committed_budget_bytes'],
            'shm_limit_bytes':s['resource_limit_bytes'],'nominal_disk_bytes_per_second':trace+checkpoint,
            'retention_bytes':load['hot_retention_nominal_bytes'],'data_raft_groups':0}

def schema_checks(openapi):
    import jsonschema
    schema=read('system-config.schema.json')
    jsonschema.Draft202012Validator.check_schema(schema)
    validator=jsonschema.Draft202012Validator(schema,format_checker=jsonschema.FormatChecker())
    reference=tomllib.loads((ROOT/'config/reference.toml').read_text())
    errors=list(validator.iter_errors(reference))
    require(not errors, 'reference config: '+ '; '.join(str(e) for e in errors[:3]))
    template=tomllib.loads((ROOT/'config/production.template.toml').read_text())
    rejected=list(validator.iter_errors(template))
    require(bool(rejected), 'production template must reject unresolved actual inputs')
    p=read('deployment-reference-v1.json'); r=read('replication-profile-v1.json')
    require(reference['identity']['deployment_profile']==p['profile_id'], 'runtime deployment profile')
    require(reference['journal']['commit_profile']==r['default_profile'], 'runtime persistence profile')
    require(reference['control']['operation_timeout_ms']==p['timing']['config_operation_default_deadline_ms'], 'config operation deadline')
    require(reference['control']['exec_grant_lifetime_ms']==p['timing']['execution_authority_lease_ms'], 'runtime exec lease')
    require(reference['control']['strategy_grant_lifetime_ms']==p['timing']['strategy_grant_lifetime_ms'], 'runtime strategy grant')
    require(reference['qualification']['full_send_p99_us']==p['slo']['normal']['complete_tick_to_socket_p99_us'], 'runtime p99')
    require(reference['qualification']['full_send_p999_us']==p['slo']['normal']['complete_tick_to_socket_p999_us'], 'runtime p999')
    require(len(reference['ipc']['topics'])==p['process_capacity']['market_topics'], 'runtime topics')
    api=read('control-api.openapi.json'); operation_ids=[]; refs=[]
    def visit(obj):
        if isinstance(obj,dict):
            if '$ref' in obj:refs.append(obj['$ref'])
            for value in obj.values():visit(value)
        elif isinstance(obj,list):
            for value in obj:visit(value)
    visit(api)
    for ref in refs:
        if ref.startswith('#/'):
            obj=api
            for token in ref[2:].split('/'):
                obj=obj[token.replace('~1','/').replace('~0','~')]
        elif '://' not in ref:
            require((CONTRACTS/ref.split('#')[0]).exists(), 'OpenAPI local reference '+ref)
    for path,item in api['paths'].items():
        for verb,operation in item.items():
            if verb.lower() in {'get','post','put','patch','delete'}:
                operation_ids.append(operation['operationId'])
    require(len(operation_ids)==len(set(operation_ids)), 'unique operation IDs')
    api_result='local_refs_and_unique_operations_verified'
    if openapi:
        from openapi_spec_validator import validate
        validate(api,base_uri=(CONTRACTS/'control-api.openapi.json').as_uri())
        api_result='openapi_spec_validator_pass'
    return {'reference_schema_errors':0,'production_template_expected_rejections':len(rejected),
            'openapi':api_result,'api_operations':len(operation_ids),'api_refs':len(refs)}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--openapi',action='store_true')
    args=parser.parse_args()
    result={'scope':'design_artifacts_only_not_runtime_tests',
            'ipc':run_helper('verify-ipc-contract.py'),
            'risk':run_helper('verify-risk-vectors.py'),
            'deployment':deployment_checks(),'configuration':schema_checks(args.openapi)}
    result['config_semantics']=run_helper('verify-config-contract.py')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':
    main()
