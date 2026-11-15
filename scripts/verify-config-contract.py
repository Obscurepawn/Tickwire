"""Validate the single-host configuration, CPU/ORS semantics, resource budgets and process projections."""
import collections, copy, hashlib, ipaddress, json, pathlib, tomllib
from decimal import Decimal
import jsonschema
from referencing import Registry, Resource

root = pathlib.Path(__file__).resolve().parents[1]
schema = json.loads((root / "specs/contracts/system-config.schema.json").read_text())
reference = tomllib.loads((root / "config/reference.toml").read_text())
template = tomllib.loads((root / "config/production.template.toml").read_text())
jsonschema.Draft202012Validator.check_schema(schema)
v = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
v.validate(reference)
template_errors = list(v.iter_errors(template))
assert template_errors, "Production input template must reject unresolved inputs"


def keyed(rows, key="id"):
    result = {r[key]: r for r in rows}
    assert len(result) == len(rows), f"duplicate {key}"
    return result


def check_cpu_list(values, require_sorted):
    assert isinstance(values, list) and len(values) >= 1, "empty cpu list"
    seen = set()
    prev = None
    for cpu in values:
        assert isinstance(cpu, int) and not isinstance(cpu, bool) and 0 <= cpu < 4096, f"cpu out of range: {cpu}"
        assert cpu not in seen, f"duplicate cpu: {cpu}"
        if require_sorted and prev is not None:
            assert prev < cpu, f"unsorted housekeeping_cpus: {values}"
        seen.add(cpu)
        prev = cpu
    return seen


def valid_ip(text):
    try:
        addr = ipaddress.ip_address(text)
    except ValueError:
        return False
    return not addr.is_unspecified


def check(c):
    assert "journal" not in c, "retired journal section present"
    hosts = keyed(c["hosts"])
    procs = keyed(c["processes"])
    accounts = keyed(c["accounts"])
    venues = keyed(c["venues"])
    policies = keyed(c["risk_policies"])
    topics = keyed(c["ipc"]["topics"])
    assert len({a["risk_domain_id"] for a in accounts.values()}) == len(accounts)

    host_hot = collections.defaultdict(set)
    host_cold = {hid: check_cpu_list(h["housekeeping_cpus"], True) for hid, h in hosts.items()}
    memory = collections.Counter()

    for p in procs.values():
        h = hosts[p["host_id"]]
        hid = h["id"]
        role = p["role"]
        assert role in {"supervisor", "md", "exec", "strategy"}, f"retired role {role}"
        assert p["numa_node"] < h["numa_nodes"]
        proc_cpus = check_cpu_list(p["cpus"], False)
        assert all(0 <= cpu < h["physical_cores"] for cpu in proc_cpus)

        proc_cold = None
        if "housekeeping_cpus" in p:
            proc_cold = check_cpu_list(p["housekeeping_cpus"], True)
            assert all(0 <= cpu < h["physical_cores"] for cpu in proc_cold)
            host_cold[hid].update(proc_cold)

        for field in ("req_cpu", "resp_cpu", "epoll_wait_ms", "max_order_age_ns", "enabled_accounts", "order_manager", "rms"):
            if field in p:
                assert role == "exec", f"{field} only allowed on exec role"
        if "reactor_cpu" in p:
            assert role == "md", "reactor_cpu only allowed on md role"
        if "worker_cpu" in p:
            assert role == "strategy", "worker_cpu only allowed on strategy role"

        pins = []
        for pin_field in ("req_cpu", "resp_cpu", "reactor_cpu", "worker_cpu"):
            if pin_field in p:
                pin = p[pin_field]
                assert isinstance(pin, int) and not isinstance(pin, bool) and 0 <= pin < 4096
                pins.append(pin)

        if "req_cpu" in p and "resp_cpu" in p:
            assert p["req_cpu"] != p["resp_cpu"], "req_cpu must not equal resp_cpu"

        for pin in pins:
            if proc_cold is not None:
                assert pin not in proc_cold, "hot CPU pin overlaps process housekeeping_cpus"
            assert pin in proc_cpus, "hot CPU pin must be included in process cpus"

        if role != "supervisor":
            proc_hot = set(proc_cpus) | set(pins)
            if proc_cold is not None:
                assert proc_hot.isdisjoint(proc_cold), "hot_cpus overlaps process housekeeping_cpus"
            assert proc_hot.isdisjoint(host_cold[hid]), "hot_cpus overlaps host housekeeping_cpus"
            assert host_hot[hid].isdisjoint(proc_hot), "shared hot CPU across processes"
            host_hot[hid].update(proc_hot)

        if "epoll_wait_ms" in p:
            assert 0 <= p["epoll_wait_ms"] <= 1000
        if "max_order_age_ns" in p:
            assert 1 <= p["max_order_age_ns"] <= 1_000_000_000
        if "order_manager" in p:
            om = p["order_manager"]
            slots = om["slots_per_strat"]
            assert 1 <= slots <= 1_048_576 and (slots & (slots - 1)) == 0, "slots_per_strat must be power of two"
            assert 2 <= om["fill_map_exponent"] <= 22
            assert 1 <= om["fill_memory_s"] <= 86_400
            assert 0 <= om["max_query_count"] <= 1024
        if "enabled_accounts" in p:
            eas = p["enabled_accounts"]
            assert 1 <= len(eas) <= 255
            seen_refs = set()
            seen_aids = set()
            for ea in eas:
                aref = ea["account_ref"]
                assert 1 <= aref <= 255 and aref not in seen_refs, "account_ref must be unique in 1..=255"
                seen_refs.add(aref)
                if "account_id" in ea:
                    aid = ea["account_id"]
                    assert aid not in seen_aids and aid in accounts
                    assert accounts[aid]["exec_process_id"] == p["id"]
                    seen_aids.add(aid)
                strats = ea["strategies"]
                assert 1 <= len(strats) <= 255 and len(set(strats)) == len(strats)
                assert all(0 <= s <= 254 for s in strats)
                if "local_ip" in ea:
                    assert valid_ip(ea["local_ip"])
                assert ea["urls"]
                for pool, tuples in ea["urls"].items():
                    assert tuples
                    for pair in tuples:
                        assert len(pair) == 2 and valid_ip(pair[0]) and 1 <= pair[1] <= 64
                if "rate_limit" in ea:
                    rl = ea["rate_limit"]
                    assert rl
                    if "orders_10s" in rl and "orders_min" in rl:
                        assert rl["orders_10s"] <= rl["orders_min"]
        if "rms" in p:
            rms = p["rms"]
            max_ord = Decimal(rms["max_order_notional"])
            max_pos = Decimal(rms["max_pos_notional"])
            ff = Decimal(rms["fat_finger_ratio"])
            assert 0 < max_ord <= max_pos
            assert 0 < ff <= Decimal("1")
            w1, w5, w10 = rms["1s"], rms["5s"], rms["10s"]
            assert 1 <= w1["max_new_orders"] <= w5["max_new_orders"] <= w10["max_new_orders"]
            assert 0 < Decimal(w1["max_order_qty"]) <= Decimal(w5["max_order_qty"]) <= Decimal(w10["max_order_qty"])

        memory[hid] += p["memory_limit_bytes"]

    for hid, hot in host_hot.items():
        assert hot.isdisjoint(host_cold[hid]), "hot_cpus overlaps housekeeping_cpus on host"
    for h in hosts.values():
        assert memory[h["id"]] + h["shm_budget_bytes"] <= h["memory_bytes"]

    for a in accounts.values():
        venue = venues[a["venue_id"]]
        policy = policies[a["risk_policy_id"]]
        owner = procs[a["exec_process_id"]]
        assert owner["role"] == "exec" and owner["activation"] == "active"
        if venue["product"] == "Spot":
            assert a["position_mode"] == a["margin_mode"] == a["asset_mode"] == "NotApplicable"
        else:
            assert (
                a["position_mode"] != "NotApplicable"
                and a["margin_mode"] != "NotApplicable"
                and a["asset_mode"] != "NotApplicable"
            )
        for o in a["instrument_overrides"]:
            assert o["symbol"] in a["allowed_symbols"]
            assert (o["expiry_epoch_ms"] > 0) == (o["contract_kind"] == "Delivery")
        assert Decimal(policy["max_order_quote"]) <= Decimal(policy["max_gross_quote"])

    states = set()
    counts = collections.Counter()
    consumer_counts = collections.Counter()
    pairs = 0
    for st in c["strategies"]:
        p = procs[st["process_id"]]
        assert p["role"] == "strategy" and p["activation"] == st["role"].lower()
        key = (st["logical_strategy_id"], st["role"])
        assert key not in states
        states.add(key)
        counts[st["role"]] += 1
        assert set(st["account_ids"]) <= accounts.keys() and set(st["topic_ids"]) <= topics.keys()
        budgets = keyed(st["risk_budgets"], "account_id")
        assert set(budgets) == set(st["account_ids"])
        for aid, b in budgets.items():
            pol = policies[accounts[aid]["risk_policy_id"]]
            assert b["quote_asset"] == pol["quote_asset"]
            assert Decimal(b["max_order_quote"]) <= Decimal(b["max_gross_quote"])
            assert Decimal(b["max_order_quote"]) <= Decimal(pol["max_order_quote"])
        pairs += len(st["account_ids"])
        consumer_counts.update(st["topic_ids"])

    for t in topics.values():
        assert procs[t["publisher_process_id"]]["role"] == "md"
        assert venues[t["venue_id"]]["product"] == t["product"]
        assert consumer_counts[t["id"]] + len(accounts) <= t["consumer_limit"]

    i = c["ipc"]
    for n in ["market_capacity", "intent_capacity", "report_capacity"]:
        assert i[n] & (i[n] - 1) == 0
    assert 0 < i["ring_high_watermark_records"] < i["ring_stop_claiming_records"] < i["market_capacity"]
    classes = keyed(i["market_arena_classes"], "class_id")
    assert sum(x["block_bytes"] * x["block_count"] for x in classes.values()) == i["market_arena_bytes_per_bank"]
    assert all(x["block_bytes"] % 128 == 0 and x["payload_limit"] <= x["block_bytes"] - 128 for x in classes.values())
    market_banks = len(topics) * i["market_banks"]
    market_cursors = sum(t["consumer_limit"] * i["market_banks"] for t in topics.values())
    market_bank_bytes = 8192 + i["market_capacity"] * i["market_record_bytes"] + i["market_arena_bytes_per_bank"]
    intent_bank_bytes = 8192 + i["intent_capacity"] * i["intent_record_bytes"]
    report_bank_bytes = 8192 + i["report_capacity"] * i["report_record_bytes"]
    banks_and_cursors = (
        market_banks * market_bank_bytes
        + market_cursors * 8192
        + pairs * (i["intent_banks"] * (intent_bank_bytes + 8192) + i["report_banks"] * (report_bank_bytes + 8192))
    )
    snapshot_owners = len(topics) + len(accounts)
    snapshots = snapshot_owners * i["snapshots_per_owner"] * i["snapshot_max_bytes"]
    projection_objects = i["config_projection_max_objects"] * (4096 + i["config_projection_max_bytes"])
    assert banks_and_cursors + snapshots + projection_objects + i["relay_budget_bytes"] <= min(
        h["shm_budget_bytes"] for h in hosts.values() if h["role"] == "trading"
    )
    ctrl = c["control"]
    assert i["resource_heartbeat_ms"] < i["resource_session_ms"]
    assert ctrl["strategy_progress_interval_ms"] < ctrl["strategy_suspect_after_ms"]
    assert c["timeouts"]["reconnect_min_ms"] < c["timeouts"]["reconnect_max_ms"]
    return dict(
        strategy_instances=sum(counts.values()),
        active=counts["Active"],
        strategy_domain_pairs=pairs,
        bank_and_cursor_bytes=banks_and_cursors,
        snapshot_bytes=snapshots,
        projection_object_bytes=projection_objects,
        total_with_relay_bytes=banks_and_cursors + snapshots + projection_objects + i["relay_budget_bytes"],
        relay_budget_bytes=i["relay_budget_bytes"],
    )


result = check(reference)
dep = json.loads((root / "specs/contracts/deployment-reference-v1.json").read_text())
assert "default_storage_profile" not in dep
assert reference["identity"]["deployment_profile"] == dep["profile_id"]
assert len(reference["hosts"]) == dep["hosts"] == 1
assert result["active"] == dep["sample_active_strategies"]
assert len(reference["accounts"]) == dep["sample_risk_domains"]
assert reference["timeouts"]["intent_ttl_us"] == dep["intent_expiry_us"]
assert result["total_with_relay_bytes"] <= dep["resource_limit_bytes"]
for quantile in ["p99", "p999"]:
    assert reference["qualification"][f"full_send_{quantile}_us"] == dep["latency_budget_us"][quantile]

wire = json.loads((root / "specs/contracts/ipc-wire-v1.json").read_text())["profiles"]["production_reference"]
for idx, row in enumerate(reference["ipc"]["market_arena_classes"]):
    for k, val in row.items():
        assert val == wire["market"]["arena_classes"][idx][k]

assert wire["deployment"]["max_strategy_instances"] == dep["max_active_strategies"]
assert schema["properties"]["strategies"]["maxItems"] == dep["max_active_strategies"]

mutations = [
    ("unknown-field", lambda c: c.update(unlisted=True)),
    ("retired-journal-section", lambda c: c.update(journal={"commit_profile": "InMemory"})),
    ("live-fixture-mode", lambda c: c["identity"].update(mode="live")),
    ("record-size", lambda c: c["ipc"].update(intent_record_bytes=512)),
    ("plaintext-credential-field", lambda c: c["accounts"][0].update(api_key="NOT-A-REAL-KEY")),
    ("cpu-id-4096", lambda c: c["processes"][2].update(req_cpu=4096)),
    ("empty-housekeeping-cpus", lambda c: c["processes"][2].update(housekeeping_cpus=[])),
    ("duplicate-housekeeping-cpus", lambda c: c["processes"][2].update(housekeeping_cpus=[2, 2])),
    ("account-ref-zero", lambda c: c["processes"][2]["enabled_accounts"][0].update(account_ref=0)),
    ("account-ref-256", lambda c: c["processes"][2]["enabled_accounts"][0].update(account_ref=256)),
]
for label, mutate in mutations:
    c = copy.deepcopy(reference)
    mutate(c)
    assert list(v.iter_errors(c)), label

semantic_mutations = [
    ("duplicate-process", lambda c: c["processes"].append(copy.deepcopy(c["processes"][0]))),
    ("unsafe-cpu-sharing", lambda c: c["processes"][4].update(cpus=c["processes"][3]["cpus"])),
    ("equal-req-resp-cpu", lambda c: c["processes"][2].update(req_cpu=10, resp_cpu=10)),
    ("hot-overlaps-process-housekeeping", lambda c: c["processes"][2].update(housekeeping_cpus=[2, 10])),
    ("hot-overlaps-host-housekeeping", lambda c: c["processes"][5].update(cpus=[4], worker_cpu=4)),
    ("unsorted-housekeeping-cpus", lambda c: c["processes"][2].update(housekeeping_cpus=[3, 2])),
    ("cpu-out-of-range", lambda c: c["processes"][2].update(req_cpu=4096)),
    ("non-power-slots-per-strat", lambda c: c["processes"][2]["order_manager"].update(slots_per_strat=1000)),
    ("duplicate-account-ref", lambda c: c["processes"][2]["enabled_accounts"][1].update(account_ref=1)),
    ("foreign-enabled-account", lambda c: c["processes"][2]["enabled_accounts"][1].update(account_id="usdm-main")),
    ("invalid-rms-notional", lambda c: c["processes"][2]["rms"].update(max_order_notional="200000.00")),
    ("invalid-rms-window", lambda c: c["processes"][2]["rms"]["5s"].update(max_new_orders=10)),
    ("wrong venue for product", lambda c: c["ipc"]["topics"][0].update(venue_id="binance-usdm")),
    ("non-power-ring", lambda c: c["ipc"].update(intent_capacity=4097)),
    ("expired resource session", lambda c: c["ipc"].update(resource_heartbeat_ms=3000)),
]
for label, mutate in semantic_mutations:
    c = copy.deepcopy(reference)
    mutate(c)
    try:
        check(c)
    except AssertionError:
        pass
    else:
        raise AssertionError("missed negative case " + label)

# Validate the independent process-file schemas.
registry = Registry().with_resource(schema["$id"], Resource.from_contents(schema))


def component(name):
    return jsonschema.Draft202012Validator(
        {"$ref": schema["$id"] + "#/$defs/" + name},
        registry=registry,
        format_checker=jsonschema.FormatChecker(),
    )


def digest(value):
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


strategy = reference["strategies"][0]
process = next(p for p in reference["processes"] if p["id"] == strategy["process_id"])
payload = {
    "identity": reference["identity"],
    "process": process,
    "host": reference["hosts"][0],
    "strategy": strategy,
    "account_views": [
        {k: val for k, val in a.items() if k not in ["credential_ref", "account_identity_ref"]}
        for a in reference["accounts"]
        if a["id"] in strategy["account_ids"]
    ],
    "secret_bindings": [],
}
global_hash = digest(reference)
projection_hash = digest(payload)
projection_object = {
    "projection_schema_version": 1,
    "global_revision": "1",
    "global_config_hash": global_hash,
    "projection_policy_revision": "1",
    "target_process_id": process["id"],
    "projection_hash": projection_hash,
    "payload": payload,
}
component("ProjectionObject").validate(projection_object)
uid = "00000000-0000-4000-8000-000000000001"
metadata = {
    "revision": "1",
    "global_config_hash": global_hash,
    "projection_hash": projection_hash,
    "projection_policy_revision": "1",
    "projection_kind": "effective",
    "target_process_id": process["id"],
    "instance_slot_id": uid,
    "operation_id": uid,
    "control_commit_ref": reference["qualification"]["deployment_manifest_ref"],
    "process_boot_id": uid,
    "applied_event_seq": "1",
    "render_version": 1,
}
projection = {"projection_schema_version": 1, "payload": payload, "_materialization": metadata}
component("ProcessProjection").validate(projection)
bad = copy.deepcopy(projection)
bad["payload"]["account_views"][0]["credential_ref"] = reference["accounts"][0]["credential_ref"]
assert list(component("ProcessProjection").iter_errors(bad))
bad = copy.deepcopy(projection)
bad["payload"]["secret_bindings"] = [
    {"purpose": "execution", "ref": reference["accounts"][0]["credential_ref"]}
]
assert list(component("ProcessProjection").iter_errors(bad))
locator = {
    "locator_version": 1,
    "generation": "1",
    "cluster_id": reference["identity"]["cluster_id"],
    "active_root": reference["local_config"]["root_dir"],
    "migration_id": uid,
    "manifest_hash": global_hash,
    "previous_locator_hash": "sha256:" + ("0" * 64),
}
locator["checksum"] = digest(locator)
component("ConfigLocator").validate(locator)
bad = copy.deepcopy(locator)
bad["active_root"] = "/var/lib/.."
assert list(component("ConfigLocator").iter_errors(bad))
print(
    json.dumps(
        {
            "reference_schema_errors": 0,
            "template_expected_errors": len(template_errors),
            "projection_and_locator": "passed",
            "capacity": result,
        },
        indent=2,
    )
)
