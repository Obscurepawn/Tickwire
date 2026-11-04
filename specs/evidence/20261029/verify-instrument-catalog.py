#!/usr/bin/env python3
"""Verify the small archived economic-catalog acceptance package (Python 3.11+).

Recomputes raw and domain-separated catalog identity using the independent
contract decoder, full baseline economics, Saved/kind9 bindings, market/JOIN and
merged-input byte continuity. The archive does not contain large ELF objects;
their identity remains bound to the recorded frozen binary manifest. Process
results and kernel samples remain recorded observations, not new attestations.
This verifier neither runs strategy/account reducers nor submits an order or
establishes global Applied. No /tmp files, network, or third-party Python modules.
"""
import hashlib
import importlib.util
import json
from decimal import Decimal
from pathlib import Path
import re
import sys


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


HERE = Path(__file__).resolve().parent
H = module("archived_inputs_codecs", HERE / "verify-strategy-inputs.py")
EC = module("independent_economic_catalog", HERE.parents[2] / "scripts/verify-instrument-catalog.py")
C = H.C
sha, decode, serde_bytes, candidate_bytes = H.sha, H.decode, H.serde_bytes, H.candidate_bytes
octets, hash_array, object_matches = H.octets, H.hash_array, H.object_matches
artifact_hash, cbor_value, number = H.artifact_hash, H.cbor_value, H.number
canonical, bank, checkpoint = H.canonical, H.bank, H.checkpoint
retained, market_members, verify_join = H.retained, H.market_members, H.verify_join
bind_command = H.bind_command


def verify_input(row, f, raw_file, workers, challenges, by_pid, by_command):
    """The established input proof checks, without the prior stage's case counts."""
    worker_hash = workers[0]["deployed_sha256"]
    def value(path):
        return decode(raw_file(path))
    C.require(value(f["proof"]) == row, "saved proof differs from log")
    challenge = octets(row["challenge"], 16)
    C.require(any(challenge) and challenge not in challenges and
              any(octets(row["request_id"], 16)), "challenge/request identity")
    challenges.add(challenge)
    C.require(type(row["pid"]) is int and row["pid"] > 0 and row["pid"] not in by_pid,
              "worker PID identity")
    C.require(row["control_epoch"] > 0 and 0 < row["started"] <= row["observed"] < row["deadline"],
              "observed request epoch/time")
    C.require(hash_array(row["worker_binary"]) == worker_hash, "proof worker binary")
    host, boot = value(f["host_config"]), value(f["bootstrap"])
    approved = host["projection_inputs"]
    identity = boot["market"]["local"]
    C.require(row["object"] == approved["object"] and row["scope"] == approved["scope"],
              "startup approval/proof binding")
    projection_raw = raw_file(f["projection"])
    projection = decode(projection_raw)
    C.require(projection_raw == candidate_bytes(projection), "canonical projection")
    binding, scope = row["object"], row["scope"]
    payload = projection["payload"]
    C.require(sha(projection_raw) == hash_array(binding["object_hash"]) and
              projection["global_revision"] == str(binding["revision"]) and
              projection["global_config_hash"] == "sha256:" + hash_array(binding["global_hash"]) and
              projection["projection_policy_revision"] == str(binding["policy_revision"]) and
              projection["projection_hash"] == "sha256:" + hash_array(binding["projection_hash"]) and
              hash_array(binding["projection_hash"]) == sha(candidate_bytes(payload)),
              "projection complete object/revision hashes")
    C.require(projection["target_process_id"] == binding["process_id"] == scope["process_id"] ==
              payload["process"]["id"] == payload["strategy"]["process_id"] and
              scope["host_id"] == payload["host"]["id"] and
              scope["cluster_id"] == payload["identity"]["cluster_id"] and
              octets(scope["namespace_id"], 16).hex() == payload["identity"]["namespace_id"].replace("-", ""),
              "projection startup scope")
    C.require(binding["revision"] == 41 and boot["factory"]["revision"] == 6,
              "global/private revision distinction")
    private_raw = raw_file(f["parameters"])
    C.require(decode(private_raw) == payload["strategy"]["params"] and
              artifact_hash(payload["process"]["artifact_ref"]) == worker_hash, "parameter/schema/image binding")
    if host.get("projection_parameters") is not None:
        C.require("schema" in f, "enabled parameters require archived schema")
        C.require(artifact_hash(payload["strategy"]["parameter_schema_ref"]) == sha(raw_file(f["schema"])),
                  "enabled parameter schema bytes")
    object_matches(private_raw, boot["factory"]["configuration"], "factory private bytes")
    cp_raw = raw_file(f["checkpoint"])
    object_matches(cp_raw, row["checkpoint"], "current checkpoint")
    cp = checkpoint(cp_raw)
    C.require(cp["source"] == row["source"] and cp["cut"] == row["cut"], "current source/cut")
    for key in ("instance", "generation", "logical_strategy"):
        C.require(cp["source"][key] == identity[key], "worker source " + key)
    C.require(hash_array(cp["source"]["binary_digest"]) == worker_hash == hash_array(identity["build_hash"]),
              "current/approved image")
    C.require(cp["source"]["clock"] == {"boot_id": identity["host_boot"], "generation": boot["clock_generation"]},
              "logical clock domain")
    C.require(cp["runtime"]["now_ns"] <= row["observed"] and
              cp["runtime"]["config"]["revision"] == 6 and
              octets(cp["runtime"]["config"]["bytes"]) == private_raw, "current runtime config/time")
    objects = {hash_array(o["digest"]): o for o in row["objects"]}
    C.require(len(objects) == len(row["objects"]) and list(objects) == sorted(objects) and
              sum(o["bytes"] for o in objects.values()) <= 64 * 1024 * 1024 and
              all(0 < o["bytes"] <= 64 * 1024 * 1024 for o in objects.values()), "bounded ordered unique closure")
    actual = {}
    for item in f["objects"]:
        obj, raw = item["object"], raw_file(item["path"])
        digest = hash_array(obj["digest"])
        C.require(digest not in actual and objects.get(digest) == obj, "archive closure membership")
        object_matches(raw, obj, "closure object")
        actual[digest] = raw
    omitted = set(objects) - set(actual)
    C.require(omitted == {worker_hash} and objects[worker_hash]["bytes"] > 1024 * 1024,
              "only fixed worker ELF may be omitted")
    if "deployed_bytes" in workers[0]:
        C.require(objects[worker_hash]["bytes"] == workers[0]["deployed_bytes"], "worker ELF length")
    seals = [(raw, decode(raw)) for raw in actual.values() if raw.startswith(b'{"version":') and "checkpoint_digest" in decode(raw)]
    C.require(len(seals) == 1, "handover seal ambiguity")
    seal_raw, seal = seals[0]
    C.require(canonical(seal_raw, "handover seal") == seal and seal["version"] == 1,
              "ordinary handover seal")
    def named(digest, size=None):
        key = hash_array(digest)
        C.require(key in actual, "missing named closure object")
        raw = actual[key]
        if size is not None:
            C.require(len(raw) == size, "named object length")
        return raw
    deps = seal["dependencies"]
    C.require([x[0] for x in deps] == list(range(2, 12)), "handover dependency kinds")
    dep = dict(deps)
    C.require(all(hash_array(h) in objects for h in dep.values()) and hash_array(dep[8]) == worker_hash,
              "named dependency closure/image")
    C.require(named(dep[9]) == raw_file(f["bootstrap"]), "named actual Bootstrap")
    if "original_join" in f:
        C.require(named(dep[11]) == raw_file(f["original_join"]), "kind 11 original JOIN binding")
    merged = retained(named(seal["input_digest"], seal["input_bytes"]), cp_raw)
    business = canonical(octets(cp["business"]), "saved application")
    C.require(business["schema"] == 1 and len(business["markets"]) == len(business["accounts"]) == 1 and
              len(cp["runtime"]["markets"]) == len(cp["runtime"]["accounts"]) == 1,
              "exactly one actual account and market")
    raw_cp = named(seal["checkpoint_digest"], seal["checkpoint_bytes"])
    C.require(raw_cp[:8] == b"TWHS0001" and len(raw_cp) == 96 + number(raw_cp, 88) and
              raw_cp[96:] == octets(business["strategy"]), "handover/business strategy bytes")
    C.require(raw_cp[8:24] == octets(identity["instance"], 16) == octets(seal["instance"], 16) == octets(seal["owner"], 16)
              and number(raw_cp, 24) == identity["generation"] == seal["generation"]
              and number(raw_cp, 32) == identity["logical_strategy"] == seal["logical_strategy"],
              "handover/current source identity")
    baseline_raw = raw_file(f["baseline"])
    object_matches(baseline_raw, row["account"]["baseline"], "observed baseline")
    object_matches(baseline_raw, approved["account"]["baseline"], "approved baseline")
    C.require(named(dep[2]) == baseline_raw, "named baseline")
    baseline = cbor_value(baseline_raw)
    C.require(len(baseline) == 4 and baseline[:2] == ["tickwire.account-baseline", 1], "baseline domain/version")
    snapshot, catalog = baseline[2:]
    expected_scope = approved["account"]["scope"]
    C.require(snapshot[0] == [octets(expected_scope["risk_domain_id"], 16), expected_scope["venue"],
              {"production": 1, "test": 2}[expected_scope["environment"]], expected_scope["account"]],
              "complete runtime account scope")
    saved_account = business["accounts"][0]
    av = canonical(octets(saved_account["view"]), "saved account")
    C.require(av["schema"] == 1 and octets(av["baseline"]) == baseline_raw, "current actual baseline bytes")
    position = len(av["records"])
    account_stream = {"identity": list(hashlib.sha256(baseline_raw).digest()), "generation": snapshot[1]}
    C.require(saved_account["stream"] == row["account"]["cursor"]["stream"] == account_stream and
              row["account"]["position"] == row["account"]["cursor"]["sequence"] == position == seal["report_position"] == number(raw_cp, 40)
              and cp["runtime"]["accounts"][0] == row["account"]["cursor"], "account cursor/position")
    account_object = b"tickwire.account-input-seal.v1" + hashlib.sha256(baseline_raw).digest()
    digest = hashlib.sha256(account_object).digest()
    for index, record in enumerate(av["records"], 1):
        record_raw = octets(record)
        rv = cbor_value(record_raw, 60 * 1024)
        C.require(len(rv) == 5 and rv[:2] == ["tickwire.account-record", 1] and
                  rv[2] == hashlib.sha256(baseline_raw).digest() and rv[3] == index, "account record prefix")
        account_object = b"tickwire.account-input-seal.step.v1" + digest + index.to_bytes(8, "little") + hashlib.sha256(record_raw).digest()
        digest = hashlib.sha256(account_object).digest()
    C.require(digest == octets(row["account"]["input_digest"], 32) == octets(seal["account_input_digest"], 32)
              and account_object == named(seal["account_input_digest"], seal["account_input_bytes"]),
              "account input prefix seal")
    header = bank(raw_file(f["market_bank"]))
    report = bank(raw_file(f["report_bank"]))
    object_matches(header, row["market"]["bank"], "observed market bank")
    object_matches(header, approved["market"]["bank"], "approved market bank")
    object_matches(report, row["report"]["header"], "observed report bank")
    C.require(bank(octets(boot["external_market"]["bank_header"], 4096), True) == header and
              bank(octets(boot["report_header"], 4096), True) == report, "Bootstrap bank/report headers")
    C.require(boot["external_market"]["endpoint"]["peer"] == approved["market"]["producer"] and
              boot["report"]["peer"] == boot["intent"]["peer"] == approved["account"]["report_owner"],
              "approved full MD/report peer identity")
    C.require(number(header, 16, 2) == 1 and number(header, 168) == row["market"]["topic"] == approved["market"]["topic_id"]
              and header[72:88] == octets(approved["market"]["producer"]["instance"], 16)
              and number(header, 160) == approved["market"]["producer"]["generation"], "actual market resource identity")
    market = business["markets"][0]
    C.require(octets(market["resource"]) == header, "current materialized market resource")
    members, sequence = market_members(octets(market["view"]), header, market["maximum_levels"])
    C.require(members == row["market"]["members"] and sequence == row["market"]["cursor"]["sequence"] and
              row["market"]["cursor"] == cp["runtime"]["markets"][0] and
              row["market"]["cursor"]["stream"] == {"identity": list(hashlib.sha256(header).digest()), "generation": number(header, 152)},
              "current market cursor/full members")
    expected_members = [[x["id"], x["version"], x["required_states"]] for x in approved["market"]["instruments"]]
    C.require(expected_members == row["market"]["join"]["members"] and [x[:2] for x in expected_members] == members and
              approved["market"]["catalog_hash"] == row["market"]["join"]["catalog_hash"] and
              approved["market"]["instrument_set_version"] == row["market"]["join"]["instrument_set_version"],
              "approved complete current/JOIN member set")
    verify_join(named(dep[11]), header, row["market"]["join"], cp["runtime"]["now_ns"], sequence, identity)
    C.require(number(report, 16, 2) == 3 and report[88:104] == octets(identity["instance"], 16)
              and report[72:88] == octets(approved["account"]["report_owner"]["instance"], 16)
              and number(report, 176) == approved["account"]["global_risk_domain_id"]
              and number(report, 200) + position == row["report"]["sequence"], "actual report owner/sequence")
    ri = row["report"]["identity"]
    C.require(ri == {"logical_strategy_id": identity["logical_strategy"], "instance_id": identity["instance"],
              "strategy_generation": identity["generation"], "risk_domain": expected_scope["risk_domain_id"]},
              "actual report routing identity")
    sub = row["report"]["subscription"]
    for key, lo, n in [("resource", 40, 16), ("channel", 56, 16)]:
        C.require(octets(sub[key], n) == report[lo:lo + n], "report subscription resource")
    for key, lo in [("channel_epoch", 136), ("ring_epoch", 144), ("feed_generation", 152)]:
        C.require(sub[key] == number(report, lo), "report subscription epoch")
    C.require(sub["instance"] == identity["instance"] and sub["generation"] == identity["generation"] and
              any(octets(sub["cursor"], 16)), "report subscription consumer")
    account = approved["account"]
    C.require(payload["strategy"]["account_ids"] == [account["account_id"]] and
              payload["strategy"]["topic_ids"] == [approved["market"]["topic_id"]] and
              len(payload["account_views"]) == len(payload["venue_views"]) == len(payload["ipc"]["topics"]) == 1,
              "complete approved singleton account/topic sets")
    av, vv, topic = payload["account_views"][0], payload["venue_views"][0], payload["ipc"]["topics"][0]
    C.require(av["id"] == account["account_id"] and av["venue_id"] == vv["id"] == account["venue_id"] and
              av["risk_domain_id"] == account["global_risk_domain_id"] and av["exec_process_id"] == account["report_owner_process_id"] and
              topic["id"] == approved["market"]["topic_id"] and topic["publisher_process_id"] == approved["market"]["publisher_process_id"] and
              topic["stream"] == "NormalizedMarket" and topic["qos"] == "StrictTrading" and topic["product"] == vv["product"],
              "explicit account/venue/topic mappings")
    C.require(vv["environment"] == payload["identity"]["environment"] == "simulated" and
              expected_scope["environment"] == "test" and vv["product"] == "Spot", "fixture product/environment")
    policies = {entry[0][0]: entry for entry in catalog[0]}
    for ident, version in members:
        C.require(ident in policies and policies[ident][0][1] == version and policies[ident][1] == 1 and
                  policies[ident][0][2] == number(header, 184) and policies[ident][0][3] == octets(approved["market"]["catalog_hash"], 32),
                  "actual catalog member product/version/hash")
    command_hash = None
    C.require(("command" in f) == ("descriptor" in f), "partial command evidence")
    if "command" in f:
        command_raw, descriptor = raw_file(f["command"]), value(f["descriptor"])
        command_hash = sha(command_raw)
        C.require(command_hash == hash_array(descriptor["command_digest"]) and
                  descriptor["recovery"]["owner"] == identity["instance"], "actual command descriptor")
        bind_command(command_raw, octets(decode(private_raw)["command"]), identity)
        by_command[command_hash] = row["pid"]
    by_pid[row["pid"]] = (row, command_hash)

    return {"checkpoint": cp, "business": business, "baseline": baseline,
            "actual": actual, "dependencies": dep, "header": header, "identity": identity,
            "seal": seal, "merged_rows": merged, "bootstrap": boot, "payload": payload,
            "approved": approved, "private": private_raw}


def retained_cut(raw, cut, source):
    """Bind an intermediate cut to the already verified actual retained prefix."""
    offset, chain, sequence, found = 68, None, None, False
    for _ in range(number(raw, 32, 4, "big")):
        size = number(raw, offset + 9, 4, "big")
        kind = raw[offset + 8]
        payload = raw[offset + 13:offset + 13 + size]
        offset += 13 + size
        if kind != 5:
            continue
        if payload[:8] == b"TWM2A001":
            anchor = checkpoint(payload[8:])
            C.require(anchor["source"] == source, "threshold retained source")
            chain, sequence = octets(anchor["cut"]["chain"], 32), anchor["cut"]["sequence"]
        elif payload[:8] == b"TWM2R001":
            encoded = payload[8:]
            row = canonical(encoded, "threshold prefix row")
            chain = hashlib.sha256(b"tickwire.strategy-input.v2" + chain + len(encoded).to_bytes(4, "big") + encoded).digest()
            sequence = row["sequence"]
        if sequence == cut["sequence"] and chain == octets(cut["chain"], 32):
            found = True
    C.require(found, "first threshold cut is not on the actual current retained chain")


def asset_tuple(asset):
    return [int(asset["id"]), int(asset["version"]), asset["symbol"], asset["exponent"]]


def instrument_tuple(entry, catalog, digest):
    assets = {x["id"]: x for x in catalog["assets"]}
    contract = entry["contract"]
    kind = {"Cash": 1, "Perpetual": 2, "Delivery": 3}[contract["kind"]]
    expected = [int(entry["id"]), int(entry["version"]), int(catalog["catalog_version"]),
                digest, entry["symbol"], *[asset_tuple(assets[entry[k]]) for k in ("base", "quote", "settlement")]]
    return expected, kind, contract.get("expiry_utc_ns")


def economic_binding(binding, product, entry, catalog, digest):
    expected, kind, expiry = instrument_tuple(entry, catalog, digest)
    C.require(len(binding) == 11 and binding[:8] == expected, "complete account instrument/asset identity")
    for raw, field in zip(binding[8:10], ("price_tick", "quantity_step")):
        C.require(isinstance(raw, list) and len(raw) == 2 and type(raw[0]) is int and
                  type(raw[1]) is int and -38 <= raw[1] <= 38, "account fixed decimal")
        # Compare exact coefficient/exponent as rational integers, without the
        # Decimal context rounding a valid 38-digit economic amount.
        wanted = Decimal(entry[field]).as_tuple()
        digits = int("".join(map(str, wanted.digits))) * (-1 if wanted.sign else 1)
        power = min(raw[1], wanted.exponent)
        C.require(raw[0] * 10 ** (raw[1] - power) == digits * 10 ** (wanted.exponent - power),
                  "account economic " + field)
    C.require(binding[10] == ([kind, int(expiry)] if expiry else [kind]) and
              product == {"Spot": 1, "LinearPerpetual": 2, "LinearFuture": 5}[entry["product"]],
              "account product/contract/expiry")


def current_bid(saved, header):
    C.require(len(saved["markets"]) == 1, "single current market")
    market = saved["markets"][0]
    raw = octets(market["view"])
    members, sequence = market_members(raw, header, market["maximum_levels"])
    C.require(len(members) == 1 and number(raw, 56 + 80, 4) > 0, "one actual nonempty bid book")
    start = 56 + 96 + 512 * number(raw, 56 + 88, 4)
    return int.from_bytes(raw[start:start + 8], "little", signed=True), sequence


def verify_catalog_fixture(event, row, f, proof, raw_file, *, development=False):
    raw = raw_file(f["raw_catalog"])
    economic = EC.canonicalize(raw)
    catalog = decode(economic)
    digest = hashlib.sha256(EC.DOMAIN + economic).digest()
    obj = {"digest": list(hashlib.sha256(raw).digest()), "bytes": len(raw)}
    C.require(event["artifact"] == obj and hash_array(event["catalog_hash"]) == digest.hex(),
              "raw artifact and independent canonical economic hashes")
    C.require(event["pid"] == row["pid"] and event["challenge"] == row["challenge"] and
              event["worker_binary"] == row["worker_binary"] and event["root"] == row["root"],
              "threshold/current process/challenge binding")
    boot, business, cp = proof["bootstrap"], proof["business"], proof["checkpoint"]
    C.require(boot["instrument_catalog"] == {"digest": obj["digest"], "bytes": list(raw)} and
              business["instrument_catalog"] == obj, "actual kind9 and current Saved catalog object")
    # The raw catalog is embedded in the named kind9 Bootstrap; it need not
    # also exist as a standalone CAS object. Saved names that embedded body.
    C.require(decode(proof["actual"][hash_array(proof["dependencies"][9])])["instrument_catalog"] ==
              boot["instrument_catalog"], "catalog body belongs to actual named kind9")
    C.require(artifact_hash(proof["payload"]["venue_views"][0]["instrument_catalog_ref"]) == sha(raw),
              "complete projection raw catalog reference")
    approved = proof["approved"]["market"]
    C.require(octets(approved["catalog_hash"], 32) == digest and
              number(proof["header"], 184) == int(catalog["catalog_version"]), "JOIN/market semantic catalog identity")
    baseline = proof["baseline"]
    snapshot, policy_catalog = baseline[2:]
    C.require(snapshot[0][1:3] == [catalog["venue"], {"Production": 1, "Test": 2}[catalog["environment"]]],
              "account/catalog venue and environment")
    assets = {int(a["id"]): a for a in catalog["assets"]}
    seen_assets = set()
    for asset in snapshot[5]:
        actual = asset[1]
        C.require(actual[0] in assets and actual == asset_tuple(assets[actual[0]]) and
                  asset[0][1] == actual[0], "complete baseline asset economics including fee asset")
        seen_assets.add(actual[0])
    instruments = {int(i["id"]): i for i in catalog["instruments"]}
    used_assets = set()
    for entry in policy_catalog[0]:
        binding, product = entry[:2]
        C.require(binding[0] in instruments, "baseline instrument missing from approved catalog")
        declared = instruments[binding[0]]
        economic_binding(binding, product, declared, catalog, digest)
        used_assets.update(int(declared[x]) for x in ("base", "quote", "settlement"))
    fee_assets = seen_assets - used_assets
    # An extra directory asset is not evidence that this account owns it.
    # Verify all actual baseline assets and report this distinction explicitly.
    extra_assets = set(assets) - used_assets
    C.require(any(assets[x]["symbol"] == "BNB" for x in extra_assets),
              "independent directory fee asset is retained")
    C.require(len(instruments) == len(policy_catalog[0]) == 1 and
              row["market"]["members"] == [[i, int(e["version"])] for i, e in instruments.items()] and
              next(iter(instruments)) == (1 << 64) - 1, "complete full-u64 actual market members")
    md = decode(raw_file(f["md_config"]))
    C.require(len(md["catalogs"]) == len(md["feeds"]) == len(md["topics"]) == 1 and
              artifact_hash(md["catalogs"][0]["reference"]) == sha(raw), "actual MD approved raw object")
    if "md_raw_catalog" in f:
        C.require(raw_file(f["md_raw_catalog"]) == raw, "MD and worker original catalog bytes")
    feed, entry = md["feeds"][0], next(iter(instruments.values()))
    actual = feed["instrument"]
    C.require(feed["source"] == "spot_test" and feed["venue_id"] == catalog["venue"] and
              feed["topics"] == [approved["topic_id"]] and catalog["market"] == "Spot", "actual feed source/product/topic")
    for field in ("id", "version", "catalog_version"):
        C.require(actual[field] == int((catalog if field == "catalog_version" else entry)[field]),
                  "actual feed complete " + field)
    C.require(octets(actual["catalog_hash"], 32) == digest and actual["symbol"] == entry["symbol"] and
              actual["expiry_utc_ns"] is None, "actual feed economic identity/contract")
    for field in ("base", "quote", "settlement"):
        a = assets[int(entry[field])]
        C.require(actual[field] == {"id": int(a["id"]), "version": int(a["version"]),
                                   "symbol": a["symbol"], "exponent": a["exponent"]}, "actual feed full asset")
    C.require(actual["price_tick"] == entry["price_tick"] == event["tick"] and
              actual["quantity_step"] == entry["quantity_step"], "actual feed economic scales")
    delta, snapshot_wire = (decode(raw_file(f[name])) for name in ("actual_first_delta", "actual_first_snapshot"))
    C.require(delta["e"] == "depthUpdate" and delta["s"] == entry["symbol"] and
              delta["b"][0][0] == snapshot_wire["bids"][0][0] == event["raw_price"] == "1.00" and
              delta["U"] <= snapshot_wire["lastUpdateId"] + 1 <= delta["u"], "actual TLS source bytes/bootstrap overlap")
    tick = Decimal(entry["price_tick"])
    bid = int(Decimal(event["raw_price"]) / tick)
    C.require(Decimal(bid) * tick == Decimal(event["raw_price"]) and bid == event["actual_bid_ticks"],
              "independent exact wire decimal to price ticks")
    threshold_raw = raw_file(f["threshold_checkpoint"])
    object_matches(threshold_raw, event["checkpoint"], "first threshold checkpoint")
    threshold = checkpoint(threshold_raw)
    C.require(threshold["source"] == cp["source"] and threshold["cut"] == event["cut"] and
              threshold["cut"]["sequence"] <= cp["cut"]["sequence"] and
              threshold["runtime"]["now_ns"] <= cp["runtime"]["now_ns"], "same source and nonregressing threshold/current cut")
    retained_cut(proof["actual"][hash_array(proof["seal"]["input_digest"])], threshold["cut"], cp["source"])
    threshold_saved = canonical(octets(threshold["business"]), "threshold Saved application")
    C.require(threshold_saved["instrument_catalog"] == obj, "first callback Saved economic object")
    private = decode(proof["private"])
    C.require(private["minimum_bid_ticks"] == event["threshold"] == 75 and private["minimum_observations"] == 1,
              "fixed threshold factory parameters")
    effects = int(bid >= event["threshold"])
    for state, saved in ((threshold, threshold_saved), (cp, business)):
        actual_bid, sequence = current_bid(saved, proof["header"])
        strategy = canonical(octets(saved["strategy"]), "actual strategy checkpoint")
        C.require(actual_bid == bid and sequence == state["runtime"]["markets"][0]["sequence"] and
                  state["runtime"]["effects"]["count"] == effects == event["effects"] and
                  strategy["observations"] > 0 and strategy["spent"] is bool(effects),
                  "actual recorded market view and one-shot strategy effect")
    C.require(event["wss_subscriptions"] == event["rest_snapshots"] == 1 and
              event["md_owner"] == "actual_Runtime_in_parent_thread" and
              event["no_gateway_submission"] is True and
              (event.get("arm_commit_rejected_before_dispatch") is True or development),
              "recorded actual transport/lifecycle scope")
    if effects:
        C.require("command" in f, "actual emitted command must be archived")
        C.require(hash_array(event["intent_digest"]) == sha(raw_file(f["command"])), "actual intent byte identity")
        cmd = H.command_value(raw_file(f["command"]))
        economic_binding(cmd[2][2], 1, entry, catalog, digest)
    else:
        C.require(event["intent_digest"] is None and "command" not in f, "below-threshold has no emitted command")
    return {"tick": event["tick"], "bid_ticks": bid, "effects": effects,
            "artifact_sha256": sha(raw), "catalog_hash": digest.hex(), "actual_extra_account_assets_checked": len(fee_assets),
            "extra_directory_assets": len(extra_assets)}


def events(log):
    return [decode(line) for line in log.decode("utf-8").splitlines() if line.startswith("{")]


def verify_rejection(event, f, raw_file):
    original_raw, changed_raw = raw_file(f["md_raw_catalog"]), raw_file(f["raw_catalog"])
    original = decode(EC.canonicalize(original_raw))
    changed = decode(EC.canonicalize(changed_raw))
    original_hash = hashlib.sha256(EC.DOMAIN + EC.canonicalize(original_raw)).digest()
    changed_hash = hashlib.sha256(EC.DOMAIN + EC.canonicalize(changed_raw)).digest()
    object_matches(changed_raw, event["artifact"], "rebound valid rejected artifact")
    C.require(original_hash != changed_hash and event["actual_catalog_hash"] == list(original_hash) and
              event["changed_catalog_hash"] == list(changed_hash), "rejection actual/rebound economic identities")
    C.require(len(original["instruments"]) == len(changed["instruments"]) == 1 and
              original["instruments"][0]["price_tick"] == "0.01" and
              changed["instruments"][0]["price_tick"] == "0.02", "specific old/new economic mismatch")
    boot = decode(raw_file(f["bootstrap"]))
    C.require(boot["instrument_catalog"] == {"digest": list(hashlib.sha256(changed_raw).digest()), "bytes": list(changed_raw)},
              "rejected Bootstrap really approved changed raw bytes")
    projection_raw = raw_file(f["projection"])
    projection = decode(projection_raw)
    C.require(projection_raw == candidate_bytes(projection) and
              projection["projection_hash"] == "sha256:" + sha(candidate_bytes(projection["payload"])) and
              artifact_hash(projection["payload"]["venue_views"][0]["instrument_catalog_ref"]) == sha(changed_raw),
              "rejected complete projection rehashed to changed raw artifact")
    md = decode(raw_file(f["md_config"]))
    C.require(artifact_hash(md["catalogs"][0]["reference"]) == sha(original_raw) and
              md["feeds"][0]["instrument"]["catalog_hash"] == list(original_hash) and
              md["feeds"][0]["instrument"]["price_tick"] == original["instruments"][0]["price_tick"],
              "actual MD retains original economics")
    baseline = cbor_value(raw_file(f["baseline"]))
    policy = baseline[3][0][0]
    C.require(policy[0][3] == changed_hash, "negative baseline hash was genuinely rebound")
    # Identity is rebound but the actual old price scale remains. Compare every
    # other economic field against a same-body view with only the old tick.
    candidate = dict(changed["instruments"][0], price_tick=original["instruments"][0]["price_tick"])
    economic_binding(policy[0], policy[1], candidate, changed, changed_hash)
    wrong = Decimal(changed["instruments"][0]["price_tick"])
    old_scale = Decimal(policy[0][8][0]).scaleb(policy[0][8][1])
    C.require(old_scale != wrong and event["root"] == f["original_root"] and
              event["error"] == "instrument catalog runtime economics" and event["worker_spawned"] is False and
              event["global_reference_rebound"] is True and event["policy_and_baseline_hash_rebound"] is True,
              "recorded economic refusal after valid hash rebinding before worker")
    return {"raw_hash_rebound": True, "baseline_declared_hash_rebound": True,
            "actual_old_tick": str(old_scale), "new_tick": str(wrong), "worker_spawned": False}


def verify_md_log(raw):
    """Validate recorded MD assertions; these are not independent network proofs."""
    rows = events(raw)
    def selected(name, count):
        result = [r for r in rows if r.get("event") == name]
        C.require(len(result) == count, "MD evidence count: " + name)
        return result
    ticks = selected("md_catalog_tick_consumed", 2)
    C.require({r["price_tick"] for r in ticks} == {"0.01", "0.5"}, "MD tick cases")
    for r in ticks:
        C.require(r["pid"] > 0 and r["applied"] > 0 and
                  Decimal(r["raw_bid"]) == Decimal(r["price_tick"]) * r["bid_ticks"] and
                  Decimal(r["raw_ask"]) == Decimal(r["price_tick"]) * r["ask_ticks"], "recorded actual MD ticks")
    r = selected("md_catalog_startup_rejected", 1)[0]
    C.require(r["connections"] == 0 and r["quota_created"] is False and r["starting"] is None and
              r["error"] == "MD catalog feed economic binding", "MD refusal before first side effect")
    r = selected("md_catalog_body_restored", 1)[0]
    C.require(r["pid"] != r["previous_pid"] and r["pid"] > 0 and r["previous_pid"] > 0 and
              r["catalog_file_exists"] is False and r["retained_body_bytes"] > 0 and
              r["checkpoint"] != r["previous_checkpoint"] and r["bid_ticks"] == 198 and r["applied"] > 0,
              "recorded retained-body cold restart")
    r = selected("md_catalog_fixed_expansion", 1)[0]
    record = r["record"]
    C.require(r["pathname_replaced"] is True and r["operation"] != r["rejected_operation"] and
              r["added_bid_ticks"] == 398 and r["added_instrument"] > (1 << 32) and r["applied"] > 0 and
              record["status"] == "complete" and record["error"] is None and
              record["operation_id"] == r["operation"] and record["bank_descriptor_hash"] == r["descriptor"] and
              record["runtime_applied"] is True and record["effective_installed"] is True,
              "recorded expansion fixed body and dual receipts")
    return 4


def verify_compatibility(document, raw_file, binaries):
    def metadata(path):
        return decode(raw_file(path), metadata=True)
    def value(path):
        return decode(raw_file(path))
    def zero(value, label):
        C.require(type(value) is int and value == 0, label)
    checks = document["checks"]
    expected = {b["name"]: b["deployed_sha256"] for b in binaries["binaries"]}
    worker = next(b for b in binaries["binaries"] if b["name"] == "tickwire-counting-worker")
    def log_path(key, fallback):
        return checks[key].get("log", str(Path(checks[key]["metadata"]).parent / fallback))
    def unchanged(meta, label):
        C.require(meta["source_manifest_verified"] is True and
                  meta["binary_hashes_before"] == meta["binary_hashes_after"] == expected,
                  label + ": source/binary binding")
    def samples(rows, allowed, label):
        C.require(isinstance(rows, list) and rows, label + ": recorded image samples missing")
        pids = set()
        for row in rows:
            C.require(type(row["pid"]) is int and row["pid"] > 0 and row["pid"] in allowed and
                      row["pid"] not in pids and row["exe_link"] == "/memfd:tickwire-executable (deleted)" and
                      row["sha256"] == worker["deployed_sha256"] and row["bytes"] == worker["deployed_bytes"] and
                      int(row["stat"].split(" ", 1)[0]) == row["pid"], label + ": recorded kernel/image identity")
            pids.add(row["pid"])
        return pids
    parameter = metadata(checks["parameter_compat"]["metadata"])
    zero(checks["parameter_compat"]["exit_code"], "parameter compatibility declared failure")
    zero(parameter["exit_code"], "parameter compatibility process failure")
    unchanged(parameter, "parameter compatibility")
    C.require(parameter["environment"]["TICKWIRE_PARAMETERS_WORKER"] == worker["deployed_path"],
              "parameter compatibility selected another worker")
    parameter_log = raw_file(log_path("parameter_compat", "parameter-compat.log")).decode("utf-8")
    C.require("test result: ok. 6 passed; 0 failed; 0 ignored;" in parameter_log,
              "six legacy parameter process tests did not pass")
    parameter_names = ["projected_parameters_reject_mismatch_before_worker_start",
                       "registered_parameters_fixture_runs_without_projection_binding",
                       "projected_parameters_reach_real_registered_consumer",
                       "projected_observation_and_bid_thresholds_change_actual_worker_effects",
                       "pending_parameter_query_holds_an_actual_queued_intent_until_fresh_proof",
                       "expired_parameter_query_rejects_queued_intent_and_reaps_worker"]
    for name in parameter_names:
        C.require(f"test {name} ... ok" in parameter_log, "missing parameter compatibility case: " + name)
    parameter_events = [decode(line) for line in parameter_log.splitlines() if line.startswith("{")]
    parameter_pids = {e[k] for e in parameter_events for k in ("pid", "worker_pid")
                      if type(e.get(k)) is int and e[k] > 0}
    samples(parameter["live_images"], parameter_pids, "parameter compatibility")
    host = metadata(checks["host_process"]["metadata"])
    zero(checks["host_process"]["exit_code"], "Host declared failure")
    zero(host["exit_code"], "Host process failure")
    C.require(host["worker"] == worker["deployed_path"], "Host selected another worker")
    host_log = raw_file(log_path("host_process", "host-process.log")).decode("utf-8")
    name = "rebuild::fresh::actual_fresh_queries_cross_the_report_floor_and_keep_callbacks_running"
    C.require(f"test {name} ... ok" in host_log and
              "test result: ok. 1 passed; 0 failed; 0 ignored;" in host_log, "Host real query regression failed")
    compatibility = metadata(checks["compatibility"]["metadata"])
    zero(checks["compatibility"]["exit_code"], "Spot compatibility declared failure")
    zero(compatibility["generate_exit_code"], "Spot generation failure")
    zero(compatibility["run_exit_code"], "Spot run failure")
    unchanged(compatibility, "Spot compatibility")
    C.require(compatibility["scenario"] == "spot-handover-custom" and
              compatibility["binary_hashes_unchanged"] is True, "Spot scenario/unchanged assertion")
    prefix = Path(checks["compatibility"]["metadata"]).parent
    result = value(str(prefix / "run-result.json"))
    report = value(str(prefix / "account-report.json"))
    C.require(result == compatibility["result"] and result["account_fill_verified"] is True and
              result["event"] == "laboratory_spot_handover_complete", "Spot complete result binding")
    C.require(type(report["account_records"]) is int and report["account_records"] > 0 and
              report["account_records"] == result["mirror_records_verified"] == result["worker_report_position_verified"],
              "account/mirror/worker complete frontier")
    C.require(report["frozen"] is True and report["input_facts_not_in_account"] == 0 and
              report["pending_orders"] == [] and len(report["orders"]) == 1, "offline account closure")
    order = report["orders"][0]
    C.require(order["terminal"] is True and order["confirmed"] is True and order["fee_reconciled"] is True and
              order["unknown"] is False and order["aborted"] is False and order["uncreated"] is False and
              int(order["remaining_quantity"]["atoms"]) == 0 and
              int(order["filled_quantity"]["atoms"]) > 0 and
              order["filled_quantity"] == order["original_quantity"] and
              order["origin_instance"] != order["manager_instance"] and
              order["origin_generation"] < order["manager_generation"], "Spot Fill/fee/ownership handover")
    proof = result["preflight"]
    C.require(proof == value(str(prefix / "handover/preflight-final-verified.json")) and
              proof["action_pending_at_s"] is True and proof["action_remains_consumed"] is True and
              proof["source_s_sequence"] < proof["source_c_sequence"] and
              proof["target_final_sequence"] > proof["target_install_sequence"], "Spot real preflight continuation")
    C.require(len({proof["target_pid"], proof["rollback_pid"], proof["source_replay_pid"]}) == 3 and
              all(type(proof[k]) is int and proof[k] > 0 for k in ("target_pid", "rollback_pid", "source_replay_pid")),
              "Spot distinct real worker/probe PIDs")
    images = value(str(prefix / "live-images.json"))["images"]
    samples(images, {proof["target_pid"]}, "Spot target")
    inputs = metadata(checks["input_compat"]["metadata"])
    zero(checks["input_compat"]["exit_code"], "declared input compatibility failure")
    zero(inputs["exit_code"], "actual input compatibility failure")
    unchanged(inputs, "input compatibility")
    C.require(inputs["environment"]["TICKWIRE_INPUTS_WORKER"] == worker["deployed_path"],
              "input compatibility selected another worker")
    input_raw = raw_file(log_path("input_compat", "input-compat.log"))
    C.require("test result: ok. 8 passed; 0 failed; 0 ignored;" in input_raw.decode("utf-8"),
              "eight original input-scope regressions")
    input_rows = events(input_raw)
    samples(inputs["live_images"], {r[k] for r in input_rows for k in ("pid", "worker_pid", "old_pid", "new_pid")
                                   if type(r.get(k)) is int and r[k] > 0}, "input compatibility")
    return {"parameters": 6, "inputs": 8, "host": 1, "spot_account_records": report["account_records"]}


def final_checks(document, raw_file, binaries, pids):
    checks = document["checks"]
    def meta(path):
        return decode(raw_file(path), metadata=True)
    def zero(value, label):
        C.require(type(value) is int and value == 0, label)
    zero(checks["contracts_exit_code"], "design contract failure")
    gate = meta(checks["full_rust"]["metadata"])
    zero(checks["full_rust"]["exit_code"], "declared Rust gate failure")
    zero(gate["exit_code"], "actual Rust gate failure")
    C.require("scripts/check-rust.sh" in gate["command"], "full workspace command")
    expected = {b["name"]: b["deployed_sha256"] for b in binaries["binaries"]}
    C.require(len(expected) == len(binaries["binaries"]) == 8, "frozen binary count")
    worker = next(b for b in binaries["binaries"] if b["name"] == "tickwire-counting-worker")
    app = document["catalog_process"]
    observed = meta(app["metadata"])
    zero(observed["exit_code"], "actual catalog process metadata failure")
    C.require(observed["source_manifest_verified"] is True and
              observed["binary_hashes_before"] == observed["binary_hashes_after"] == expected and
              observed["environment"]["TICKWIRE_CATALOG_WORKER"] == worker["deployed_path"],
              "actual catalog run selected frozen source/image")
    seen = set()
    for sample in observed["live_images"]:
        C.require(sample["pid"] in pids and sample["pid"] not in seen and
                  sample["exe_link"] == "/memfd:tickwire-executable (deleted)" and
                  sample["sha256"] == worker["deployed_sha256"] and sample["bytes"] == worker["deployed_bytes"] and
                  int(sample["stat"].split(" ", 1)[0]) == sample["pid"], "recorded actual process memfd identity")
        seen.add(sample["pid"])
    C.require(seen == set(pids), "both successful catalog consumers have actual image samples")
    source = document["source_replay"]
    sm = meta(source["metadata"])
    zero(sm["exit_code"], "actual source-replay process failure")
    if "exit_code" in source:
        zero(source["exit_code"], "declared source-replay failure")
    # Final fixture semantics are checked below from original named objects;
    # these remain the original process's recorded completion/negative outcomes.
    source_checks = verify_source_archive(source, raw_file, binaries)
    compatibility = verify_compatibility(document, raw_file, binaries)
    return {"full_rust": True, "contracts": True, "actual_memfd_samples": len(seen),
            "source_replay": source_checks, "compatibility": compatibility}


def source_tail(raw, anchor, current):
    C.require(68 <= len(raw) <= 16 * 1024 * 1024 and raw[:8] == b"TWINP002" and
              raw[8:24] == octets(anchor["source"]["input_stream"], 16) and
              number(raw, 24, order="big") == anchor["cut"]["sequence"] + 1 and
              raw[32:64] == octets(anchor["cut"]["chain"], 32), "source tail anchor/header")
    count = number(raw, 64, 4, "big")
    C.require(count <= 65536, "source row count")
    offset, chain, when, rows = 68, raw[32:64], anchor["runtime"]["now_ns"], []
    for index in range(count):
        size = number(raw, offset, 4, "big")
        offset += 4
        C.require(size <= len(raw) - offset, "source tail row length")
        encoded = raw[offset:offset + size]
        row = canonical(encoded, "source merged input")
        C.require(row["sequence"] == anchor["cut"]["sequence"] + index + 1 and
                  row["now_ns"] >= when, "source row sequence/time")
        chain = hashlib.sha256(b"tickwire.strategy-input.v2" + chain + size.to_bytes(4, "big") + encoded).digest()
        when = row["now_ns"]
        offset += size
        rows.append(row)
    C.require(offset == len(raw) and current["cut"]["sequence"] == anchor["cut"]["sequence"] + count and
              octets(current["cut"]["chain"], 32) == chain and current["runtime"]["now_ns"] == when,
              "source tail complete current frontier")
    return rows


def verify_source_archive(source, raw_file, binaries):
    C.require(len(source["fixtures"]) == 1, "one actual source replay fixture")
    f = source["fixtures"][0]
    facts = decode(raw_file(f["evidence"]))
    logged = [r for r in events(raw_file(source["log"])) if r.get("event") == "catalog_source_replay_verified"]
    C.require(logged == [facts], "source original evidence/log equality")
    names = ("job", "kind9", "capture", "anchor", "tail", "checkpoint", "source_receipt")
    for name in names:
        object_matches(raw_file(f[name]), facts[name], "source " + name)
    job = canonical(raw_file(f["job"]), "source job")
    receipt = canonical(raw_file(f["source_receipt"]), "source receipt")
    request = canonical(raw_file(f["request"]), "source capture request")
    capture = canonical(raw_file(f["capture"]), "source captured tail")
    anchor, current = (checkpoint(raw_file(f[name])) for name in ("anchor", "checkpoint"))
    body = canonical(raw_file(f["kind9"]), "original kind9")
    for name in ("request", "capture", "anchor", "tail", "checkpoint"):
        object_matches(raw_file(f[name]), job[name]["object"], "job original " + name)
        C.require(Path(job[name]["path"]).is_absolute(), "original source input path")
    C.require(job["catalog_bootstrap"]["object"] == facts["kind9"] and
              receipt["job"] == facts["job"] and receipt["capture"] == facts["capture"] and
              receipt["output"] == facts["checkpoint"] and receipt["binding"] == job["binding"] and
              receipt["source"] == job["source"] == capture["source"] == request["source"] ==
              anchor["source"] == current["source"] == facts["source"], "source full request/receipt identity")
    C.require(job["schema"] == receipt["schema"] == capture["schema"] == 1 and
              request["kind"] == "capture" and request["pause"] is capture["paused"] is True and
              request["anchor"] == capture["anchor"] == facts["anchor"] and
              request["expected"] == anchor["cut"] == facts["cut_s"] and
              capture["request"] == job["request"]["object"] and capture["tail"] == facts["tail"] and
              capture["checkpoint"] == facts["checkpoint"] and
              receipt["cut"] == capture["cut"] == current["cut"] == facts["cut_c"], "source capture graph")
    worker = [b for b in binaries["binaries"] if b["name"] == "tickwire-strategy-worker"]
    C.require(len(worker) == 1 and hash_array(job["source"]["binary_digest"]) == worker[0]["deployed_sha256"] ==
              hash_array(facts["worker_sha256"]) and facts["worker"] == worker[0]["deployed_path"] and
              type(receipt["pid"]) is int and receipt["pid"] == facts["pid"] > 0, "source actual selected executable/PID")
    id_ = body["market"]["local"]
    for field in ("instance", "generation", "logical_strategy"):
        C.require(id_[field] == job["source"][field], "original source Bootstrap identity")
    C.require(id_["build_hash"] == job["source"]["binary_digest"] and
              job["source"]["clock"] == {"boot_id": id_["host_boot"], "generation": body["clock_generation"]},
              "original source Bootstrap image/clock")
    raw_catalog = octets(body["instrument_catalog"]["bytes"])
    object_matches(raw_catalog, facts["catalog"], "source embedded original catalog")
    C.require(hash_array(body["instrument_catalog"]["digest"]) == sha(raw_catalog), "source original raw approval")
    economic = decode(EC.canonicalize(raw_catalog))
    digest = hashlib.sha256(EC.DOMAIN + EC.canonicalize(raw_catalog)).digest()
    for cp in (anchor, current):
        saved = canonical(octets(cp["business"]), "source saved application")
        C.require(saved["instrument_catalog"] == facts["catalog"], "source S/C Saved object binding")
        declared = {int(e["id"]): int(e["version"]) for e in economic["instruments"]}
        for market in saved["markets"]:
            header = bank(octets(market["resource"]))
            members, _ = market_members(octets(market["view"]), header, market["maximum_levels"])
            C.require(number(header, 184) == int(economic["catalog_version"]) and
                      all(declared.get(i) == version for i, version in members),
                      "source current/anchor market economic membership")
        for account in saved["accounts"]:
            av = canonical(octets(account["view"]), "source account view")
            base = cbor_value(octets(av["baseline"]))
            assets = {int(a["id"]): a for a in economic["assets"]}
            C.require(base[2][0][1:3] == [economic["venue"], {"Test": 2, "Production": 1}[economic["environment"]]],
                      "source account/catalog scope")
            for asset in base[2][5]:
                C.require(asset[1][0] in assets and asset[1] == asset_tuple(assets[asset[1][0]]), "source complete asset")
            entries = {int(e["id"]): e for e in economic["instruments"]}
            for policy in base[3][0]:
                economic_binding(policy[0], policy[1], entries[policy[0][0]], economic, digest)
    rows = source_tail(raw_file(f["tail"]), anchor, current)
    C.require(len(rows) > 0 and current["runtime"]["effects"]["count"] - anchor["runtime"]["effects"]["count"] ==
              facts["historical_effects"] == 1 and facts["replayed_commands_directory"] is False and
              "not authenticated live HandoverSeals" in facts["scope"], "source replay exact evidence boundary")
    missing = canonical(raw_file(f["missing_reference"]), "missing reference job")
    absent = canonical(raw_file(f["missing_object"]), "missing object job")
    # Keep original requests unchanged: output directories differ, and only the
    # missing-reference request omits catalog_bootstrap. Missing-file state is a
    # recorded process outcome; the successful body was restored for archiving.
    for negative in (missing, absent):
        C.require(negative["output_directory"] != job["output_directory"], "isolated negative output")
        for field in job:
            if field not in ("output_directory", "catalog_bootstrap"):
                C.require(negative[field] == job[field], "negative source request identity")
    C.require(missing.get("catalog_bootstrap") is None and absent["catalog_bootstrap"] == job["catalog_bootstrap"] and
              type(facts["missing_reference_exit"]) is int and facts["missing_reference_exit"] != 0 and
              type(facts["missing_object_exit"]) is int and facts["missing_object_exit"] != 0,
              "actual missing original dependency negatives")
    return {"recorded_child_outcomes": 3, "merged_rows": len(rows),
            "boundary": "Captured checkpoints and original kind9; not authenticated live HandoverSeals."}


def verify(manifest_path):
    base = manifest_path.resolve().parent
    def read(relative, limit=32 * 1024 * 1024):
        C.require(isinstance(relative, str), "evidence path type")
        p = Path(relative)
        C.require(not p.is_absolute() and ".." not in p.parts, "nonlocal evidence path")
        p = base / p
        C.require(p.resolve().is_relative_to(base) and p.is_file() and
                  not any(x.is_symlink() for x in (p, *p.parents) if x != base), "evidence path boundary")
        C.require(0 <= p.stat().st_size <= limit, "evidence file bounds")
        return p.read_bytes()
    document = decode(read(manifest_path.name, 4 * 1024 * 1024))
    C.require(document["schema"] == 1, "archive schema")
    archive, total = {}, 0
    for name, binding in document["artifacts"].items():
        raw = read(name)
        C.require(len(raw) == binding["bytes"] and sha(raw) == binding["sha256"], "artifact changed: " + name)
        total += len(raw)
        C.require(total <= 128 * 1024 * 1024, "archive exceeds 128 MiB")
        archive[name] = raw
    def raw_file(name):
        C.require(name in archive, "unlisted archive artifact: " + name)
        return archive[name]
    source = document["source_binding"]
    C.require(sha(read(source["manifest"], 4 * 1024 * 1024)) == source["manifest_sha256"], "source manifest hash")
    binaries = decode(raw_file(document["build"]["binary_manifest"]))
    C.require(binaries["build_exit_code"] == 0 and
              binaries["source_manifest_sha256"] == source["manifest_sha256"], "build/source binding")
    workers = [b for b in binaries["binaries"] if b["name"] == "tickwire-counting-worker"]
    C.require(len(workers) == 1, "ambiguous frozen worker")
    app = document["catalog_process"]
    C.require(app["exit_code"] == 0, "catalog actual process failure")
    raw_log = raw_file(app["log"])
    log = raw_log.decode("utf-8")
    C.require("test result: ok. 2 passed; 0 failed; 0 ignored;" in log and
              "test same_raw_price_uses_economic_tick_before_actual_strategy_threshold ... ok" in log and
              "test gates::rehashed_catalog_and_projection_cannot_override_actual_old_economics ... ok" in log,
              "catalog actual process summary")
    rows = events(raw_log)
    consumed = [r for r in rows if r.get("case") == "input_scope_consumed"]
    threshold = {r["root"]: r for r in rows if r.get("case") == "catalog_actual_price_threshold"}
    fixtures = {f["original_root"]: f for f in app["fixtures"]}
    C.require(len(consumed) == len(threshold) == len(fixtures) == len(app["fixtures"]) == 2 and
              set(threshold) == set(fixtures) == {r["root"] for r in consumed}, "two complete actual economic cases")
    challenges, by_pid, by_command, results = set(), {}, {}, []
    merged_count = 0
    for row in consumed:
        f = fixtures[row["root"]]
        proof = verify_input(row, f, raw_file, workers, challenges, by_pid, by_command)
        event = threshold[row["root"]]
        result = verify_catalog_fixture(event, row, f, proof, raw_file,
                                        development=document.get("development_only", False))
        if event.get("arm_commit_rejected_before_dispatch") is not True:
            result["lifecycle_check"] = "NOT_CHECKED_DEVELOPMENT_LOG"
        results.append(result)
        merged_count += len(proof["merged_rows"])
        params = [r for r in rows if r.get("case") == "parameter_consumed" and r.get("pid") == row["pid"]]
        C.require(len(params) == 1 and params[0]["challenge"] == row["challenge"] and
                  params[0]["checkpoint"] == row["checkpoint"] and params[0]["current_cut"] == row["cut"] and
                  params[0]["global_revision"] == 41 and params[0]["private_revision"] == 6,
                  "actual parameters and input scope share one current challenge")
    C.require({(r["tick"], r["bid_ticks"], r["effects"]) for r in results} == {("0.01", 100, 1), ("0.02", 50, 0)} and
              len({r["catalog_hash"] for r in results}) == 2, "actual distinct economics and behavior")
    rejections = []
    rejected = [r for r in rows if r.get("case") == "catalog_rehashed_economics_refused"]
    if not document.get("development_only", False) or app.get("rejections"):
        C.require(len(app["rejections"]) == len(rejected) == 1, "complete rehashed-economics refusal evidence")
        rejections = [verify_rejection(rejected[0], app["rejections"][0], raw_file)]
    md_cases = verify_md_log(raw_file(document["md_process"]["log"])) if "md_process" in document else None
    final = None
    if not document.get("development_only", False):
        C.require(md_cases == 4, "final package missing MD regressions")
        final = final_checks(document, raw_file, binaries, by_pid)
    C.require(document["scope"]["gateway_submission"] is False and document["scope"]["global_applied"] is False,
              "archive scope overclaim")
    return {"result": "PASS", "mode": "development" if document.get("development_only") else "archived",
            "checks": C.count, "archived_files": len(archive), "catalog_cases": results,
            "input_proofs": len(consumed), "merged_rows": merged_count, "md_recorded_cases": md_cases,
            "rehashed_economic_refusals": rejections,
            "final_checks": final, "gateway_submission": False, "global_applied": False,
            "boundary": "Archived byte bindings and recorded outcomes; no execution, economic replay, kernel reauthentication, or ELF rehash."}


if __name__ == "__main__":
    try:
        p = Path(sys.argv[1]) if len(sys.argv) == 2 else HERE / "instrument-catalog.json"
        if len(sys.argv) > 2:
            raise ValueError("usage: verify-instrument-catalog.py [manifest.json]")
        print(json.dumps(verify(p), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, IndexError, RecursionError, OverflowError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
