#!/usr/bin/env python3
"""Recheck this frozen MD restart archive using Python 3.11+ standard library.

Checks exact archived bytes, recorded process images, and complete recovery CAS.
Never opens runtime paths embedded in records or the current repository sources.
This is historical content verification, not a network rerun, launch authorization,
proof of crash behavior beyond the recorded tests, or a performance qualification.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys

MAX_OBJECT = 16 << 20
MAX_TOTAL = 512 << 20
PHASES = {"starting", "ready", "reserved", "revoking", "applied"}
IDS = [2**64 - 1, 2**63 + 1001, 2**64 - 2]
BINARIES = {"tickwire-lab", "tickwire-app", "tickwire-md", "tickwire-strategy-worker",
            "tickwire-replica-receiver", "tickwire-lifecycle", "tickwire-counting-v1-worker",
            "tickwire-counting-worker"}


def require(ok, why):
    if not ok:
        raise ValueError(why)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda x: (_ for _ in ()).throw(ValueError(f"JSON {x}")))


def compact(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def fields(value, names, optional=()):
    expected = names.split()
    require(isinstance(value, dict), f"expected object: {names}")
    require(list(value) == [k for k in expected if k in value]
            and set(expected) - set(value) <= set(optional), f"noncanonical fields: {names}")


def uint(value, bits=64, nonzero=False):
    require(type(value) is int and int(nonzero) <= value < 1 << bits, f"invalid u{bits}: {value!r}")
    return value


def octets(value, width, label, nonzero=False):
    require(isinstance(value, list) and len(value) == width, f"{label}: byte width")
    require(all(type(v) is int and 0 <= v <= 255 for v in value), f"{label}: byte value")
    raw = bytes(value)
    require(not nonzero or any(raw), f"{label}: zero identity")
    return raw


def h(value, label):
    return octets(value, 32, label).hex()


def canonical(raw, maximum):
    require(0 < len(raw) <= maximum, "canonical JSON byte budget")
    value = decode(raw)
    def integers_only(item):
        require(not isinstance(item, float), "floating point in recovery content")
        if isinstance(item, dict):
            for child in item.values():
                integers_only(child)
        elif isinstance(item, list):
            for child in item:
                integers_only(child)
    integers_only(value)
    require(compact(value) == raw, "noncanonical recovery JSON bytes")
    return value


def config_order(c):
    # Frozen serde field order. This checks encoding, not deployment admission.
    fields(c, "identity clock_generation socket admission_directory quota_directory quota control_directory control_stream control_domain control_writer_epoch feeds topics maximum_idle_ns expansion")
    fields(c["identity"], "instance generation role logical_strategy host_boot namespace build_hash qualification_hash manifest_hash arch trust_profile")
    for feed in c["feeds"]:
        fields(feed, "owner_id source instrument venue_id stream_id first_connection_epoch first_operation initial_book_generation snapshot_limit stream_address snapshot_address trust_anchors sbe_api_key_file topics events")
        fields(feed["instrument"], "id version catalog_version catalog_hash symbol base quote settlement price_tick quantity_step expiry_utc_ns")
        for name in ("base", "quote", "settlement"):
            fields(feed["instrument"][name], "id version symbol exponent")
        for event in feed["events"]:
            fields(event, "owner_id stream_id kind first_connection_epoch first_operation stream_address sbe_api_key_file fast_mark")
    for topic in c["topics"]:
        fields(topic, "resource instrument_set_version maximum_levels maximum_cache_bytes maximum_snapshot_bytes maximum_delta_bytes")
    expansion = c["expansion"]
    fields(expansion, "recovery plan_path result_path effective_path consumer_directory initial_revision")
    fields(expansion["recovery"], "directory restart_from", ("restart_from",))
    q = c["quota"]
    fields(q, "supervisor_generation consumer_instance consumer_generation consumer_lease_until_ns producer_id policy_generation policy_starts_ns policy_ends_ns windows grants rules")
    for window in q["windows"]:
        fields(window, "name namespace scope subject pool window revision starts_ns ends_ns limit recovery_reserve")
    for grant in q["grants"]:
        fields(grant, "request_id class expires_ns costs")
    for rule in q["rules"]:
        fields(rule, "source action class api_weight websocket_messages costs")
        # This archive's MD budget contains public stream/snapshot actions only.
        fields(rule["action"], "family lane kind")
        require(rule["action"]["family"] == "public", "unexpected archived MD quota family")
    for row in q["grants"] + q["rules"]:
        for cost in row["costs"]:
            fields(cost, "window units")


def bank_header(value):
    raw = octets(value, 4096, "bank")
    def n(at, size=8):
        return int.from_bytes(raw[at:at + size], "little")
    require(raw[:8] == b"TWIPC001" and (n(8, 2), n(10, 2), n(12, 4)) == (1, 0, 4096), "bank ABI")
    require(n(16, 2) == 1 and n(18, 2) in (1, 2) and raw[20:24] == bytes([1, 8, 12, 7]), "bank platform/kind")
    require(not any(raw[368:512]) and not any(raw[768:]), "bank reserved bytes")
    descriptor = raw[272:304]
    require(not any(descriptor) or hashlib.sha256(raw[:272] + bytes(32) + raw[304:]).digest() == descriptor, "bank descriptor SHA-256")
    cap, width = n(28, 4), n(24, 4)
    require(64 <= cap <= 1048576 and cap & (cap - 1) == 0 and width == 512, "bank ring geometry")
    require((n(232), n(256), n(264)) == (8192, 4096, 0), "bank page offsets")
    for at in (40, 56, 72):
        require(any(raw[at:at + 16]), "bank zero resource/channel/producer")
    for at in (136, 144, 152, 160, 200, 224):
        uint(n(at), 63, True)
    require(n(216) == n(200) - 1 and n(208) < n(144)
            and (n(208) != 0 or n(216) == 0), "bank predecessor/cut")
    flags = n(304)
    require(flags & 0x1c == 0x1c and flags & ~0x3df == 0, "bank required features")
    require(1 <= n(324, 4) <= 32 and (n(330, 2), n(332, 2), n(364, 4)) == (1, 0, 1), "bank schema/consumers/clock")
    count, cursor, prior, blocks = n(360, 2), 8192 + cap * width, 192, 0
    require(0 <= count <= 4, "bank arena class count")
    for i in range(4):
        at = 512 + 64 * i
        require(not any(raw[at + 2:at + 4]) and not any(raw[at + 32:at + 64]), "arena reserved bytes")
        if i >= count:
            require(not any(raw[at:at + 64]), "unused arena class")
            continue
        block_bytes, block_count, limit = n(at + 4, 4), n(at + 8, 4), n(at + 12, 4)
        require(n(at, 2) == i + 1 and prior < limit <= 65536 and block_count > 0
                and 128 + limit <= block_bytes <= 65664 and block_bytes % 128 == 0
                and n(at + 16) == cursor and n(at + 24) == block_bytes * block_count, "arena class geometry")
        cursor += block_bytes * block_count
        blocks += block_count
        prior = limit
    require(cursor == n(32) and blocks <= cap, "bank arena total")
    if count:
        require(flags & 2 and n(240) == 8192 + cap * width and n(248) == cursor - n(240), "bank arena extent")
    else:
        require(not flags & 2 and n(240) == n(248) == 0, "bank absent arena")
    return {"raw": raw, "resource": raw[40:56], "channel": raw[56:72], "producer": raw[72:88],
            "host_boot": raw[104:120], "namespace": raw[120:136], "arch": n(18, 2),
            "trust": n(362, 2), "generation": n(160), "topic": n(168), "catalog": n(184),
            "ring": n(144), "feed": n(152), "descriptor": descriptor}


def snapshot(raw):
    s = canonical(raw, MAX_OBJECT)
    fields(s, "schema sequence previous phase revision config_hash config instrument_ids banks operations reserved_owners")
    require(uint(s["schema"], 16) == 1 and s["phase"] in PHASES, "snapshot schema/phase")
    uint(s["sequence"], 63, True)
    uint(s["revision"], 63, True)
    h(s["previous"], "predecessor")
    c = s["config"]
    config_order(c)
    require(sha(compact(c)) == h(s["config_hash"], "config hash"), "snapshot Config hash")
    ids = s["instrument_ids"]
    require(1 <= len(ids) <= 32 and len(ids) == len(set(ids)), "dense ID count/duplicates")
    for identity in ids:
        uint(identity, 64, True)
    require(1 <= len(c["feeds"]) <= 32 and set(ids) == {f["instrument"]["id"] for f in c["feeds"]}, "dense ID/feed coverage")
    require(1 <= len(c["topics"]) <= 32 and len(c["topics"]) == len(s["banks"]), "snapshot bank/topic count")
    require(len(s["operations"]) <= 128 and len(s["reserved_owners"]) <= 16384, "history capacity")
    operations = {}
    for operation in s["operations"]:
        fields(operation, "operation_id approval_digest")
        key = octets(operation["operation_id"], 16, "operation", True)
        require(key not in operations, "duplicate operation")
        operations[key] = octets(operation["approval_digest"], 32, "approval digest", True)
    owners = [octets(v, 16, "reserved owner", True) for v in s["reserved_owners"]]
    require(len(owners) == len(set(owners)), "duplicate reserved owner")
    actual_owners = []
    for feed in c["feeds"]:
        require(len(feed["events"]) <= 3 and 1 <= len(feed["topics"]) <= 32
                and len(set(feed["topics"])) == len(feed["topics"]), "feed topic/event bounds")
        actual_owners.append(octets(feed["owner_id"], 16, "depth owner", True))
        actual_owners.extend(octets(e["owner_id"], 16, "event owner", True) for e in feed["events"])
    require(len(actual_owners) == len(set(actual_owners)) and set(actual_owners) <= set(owners), "live source owner reservation")
    banks = [bank_header(v) for v in s["banks"]]
    require(s["phase"] != "ready" or all(any(b["descriptor"]) for b in banks), "Ready bank lacks actual descriptor digest")
    require(len({b["topic"] for b in banks}) == len(banks)
            and len({b["resource"] for b in banks}) == len(banks)
            and len({t["resource"] for t in c["topics"]}) == len(banks), "duplicate topic/resource/path")
    identity = c["identity"]
    producer = (octets(identity["instance"], 16, "producer", True), uint(identity["generation"], 63, True),
                octets(identity["host_boot"], 16, "host boot", True))
    for bank, topic in zip(banks, c["topics"]):
        uint(topic["instrument_set_version"], 63, True)
        require((bank["producer"], bank["generation"], bank["host_boot"]) == producer
                and bank["namespace"] == octets(identity["namespace"], 16, "namespace", True)
                and bank["arch"] == identity["arch"] and bank["trust"] == identity["trust_profile"], "bank/config producer identity")
        members = [f["instrument"] for f in c["feeds"] if bank["topic"] in f["topics"]]
        require(members and len({m["id"] for m in members}) == len(members), "bank instrument membership")
        require(all(m["catalog_version"] == bank["catalog"] for m in members)
                and len({octets(m["catalog_hash"], 32, "catalog hash", True) for m in members}) == 1, "bank catalog binding")
    topics = {b["topic"] for b in banks}
    require(all(set(f["topics"]) <= topics for f in c["feeds"]), "unknown feed topic")
    return {"value": s, "banks": banks, "operations": operations, "owners": set(owners),
            "actual_owners": set(actual_owners), "producer": producer, "hash": sha(raw)}


def history(archive, prefix):
    names = {name[len(prefix) + 1:]: raw for name, raw in archive.items() if name.startswith(prefix + "/")}
    require("HEAD" in names and 1 <= len(names) - 1 <= 1024, f"{prefix}: HEAD/object count")
    require(all(name == "HEAD" or re.fullmatch(r"object-[0-9a-f]{64}", name) for name in names), f"{prefix}: unexpected file")
    require(sum(map(len, names.values())) <= MAX_TOTAL + 512, f"{prefix}: total byte budget")
    head = canonical(names["HEAD"], 512)
    fields(head, "schema sequence digest bytes")
    require(uint(head["schema"], 16) == 1 and uint(head["sequence"], 63, True) == len(names) - 1, f"{prefix}: HEAD sequence/count")
    uint(head["bytes"], 64, True)
    digest, sequence, chain = h(head["digest"], "HEAD digest"), head["sequence"], []
    seen = set()
    while sequence:
        require(digest not in seen and f"object-{digest}" in names, f"{prefix}: cycle/missing predecessor {digest}")
        seen.add(digest)
        raw = names[f"object-{digest}"]
        require(sha(raw) == digest, f"{prefix}: object digest {digest}")
        if not chain:
            require(len(raw) == head["bytes"], f"{prefix}: HEAD length")
        entry = snapshot(raw)
        s = entry["value"]
        require(s["sequence"] == sequence, f"{prefix}: nonconsecutive sequence")
        chain.append(entry)
        sequence -= 1
        digest = h(s["previous"], "previous")
    require(digest == "00" * 32 and len(seen) == len(names) - 1, f"{prefix}: truncated/orphan history")
    chain.reverse()
    require(chain[0]["value"]["phase"] == "starting", f"{prefix}: initial phase")
    resources, channels = set(), set()
    previous = None
    for entry in chain:
        s = entry["value"]
        if previous:
            p = previous["value"]
            require(s["revision"] >= p["revision"] and s["instrument_ids"][:len(p["instrument_ids"])] == p["instrument_ids"], f"{prefix}: revision/dense prefix changed")
            require(previous["operations"].items() <= entry["operations"].items()
                    and previous["owners"] <= entry["owners"], f"{prefix}: consumed history changed")
            if entry["producer"] != previous["producer"]:
                require(p["phase"] == "ready" and s["phase"] == "starting" and p["revision"] == s["revision"], f"{prefix}: incomplete producer transition")
                require(entry["producer"][0] != previous["producer"][0] and entry["producer"][1] > previous["producer"][1], f"{prefix}: producer reused")
                require(entry["actual_owners"].isdisjoint(previous["owners"]), f"{prefix}: source owner reused at restart")
                require(all(b["resource"] not in resources and b["channel"] not in channels for b in entry["banks"]), f"{prefix}: restart bank reused")
                approved = s["config"]["expansion"]["recovery"].get("restart_from")
                require(h(approved, "approved restart") == previous["hash"], f"{prefix}: restart must select exact HEAD")
            else:
                current = {b["topic"]: b for b in entry["banks"]}
                for bank in previous["banks"]:
                    require(bank["topic"] in current, f"{prefix}: removed bank")
                    after = current[bank["topic"]]
                    require(after["ring"] >= bank["ring"] and after["feed"] >= bank["feed"], f"{prefix}: bank frontier regressed")
                    if after["ring"] == bank["ring"]:
                        require(after["raw"] == bank["raw"], f"{prefix}: same epoch changed bank")
                    else:
                        require(after["resource"] not in resources, f"{prefix}: retired bank resource reused")
        resources.update(b["resource"] for b in entry["banks"])
        channels.update(b["channel"] for b in entry["banks"])
        previous = entry
    return chain


def verify(report_path=None):
    path = Path(report_path) if report_path else Path(__file__).with_name("md-restart.json")
    base, report = path.resolve().parent, decode(path.read_bytes())
    archive = {}
    manifest = report["source_manifest"]
    entries = list(report["artifacts"])
    if not any(item["path"] == manifest["path"] for item in entries):
        entries.append(manifest)
    for item in entries:
        name = item["path"]
        relative = PurePosixPath(name)
        require(not relative.is_absolute() and str(relative) == name and ".." not in relative.parts, f"unsafe artifact path: {name}")
        require(name not in archive, f"duplicate artifact: {name}")
        uint(item["bytes"])
        target = base.joinpath(*relative.parts)
        require(target.resolve().is_relative_to(base) and not target.is_symlink(), f"artifact escapes archive: {name}")
        require(target.stat().st_size == item["bytes"] and item["bytes"] <= 64 << 20, f"artifact length: {name}")
        raw = target.read_bytes()
        require(len(raw) == item["bytes"] and sha(raw) == item["sha256"], f"artifact digest: {name}")
        archive[name] = raw
    def value(name):
        require(name in archive, f"unlisted required artifact: {name}")
        return decode(archive[name])
    source_name = report["source_manifest"]["path"]
    source = value(source_name)
    require(len(source["files"]) == source["file_count"] == report["build_input_count"], "source input count")
    require(len({r["path"] for r in source["files"]}) == source["file_count"], "duplicate source path")
    require(sha(archive[source_name]) == report["source_manifest"]["sha256"], "source manifest digest")
    binaries = value("md-restart/binaries.json")
    require(binaries["schema"] == 1 and binaries["profile"] == "release" and binaries["build_exit_code"] == 0, "release build metadata")
    require(binaries["source_manifest_sha256"] == sha(archive[source_name]), "binary/source binding")
    rows = binaries["binaries"]
    require(len(rows) == 8 and {b["name"] for b in rows} == BINARIES, "fixed binary inventory")
    for row in rows:
        for kind in ("build", "deployed"):
            require(re.fullmatch(r"[0-9a-f]{64}", row[f"{kind}_sha256"]), "binary digest format")
            uint(row[f"{kind}_bytes"], 64, True)
    md = next(b for b in rows if b["name"] == "tickwire-md")
    cli = value("md-restart/cli-acceptance.json")
    require(cli["exit_code"] == 0 and cli["source_and_binary_unchanged"] is True, "CLI acceptance failed/drifted")
    require(cli["binary_sha256"] == md["deployed_sha256"] and cli["binary"] == md["deployed_path"], "CLI selected binary")
    samples = {}
    for sample in cli["samples"]:
        pid = uint(sample["pid"], 32, True)
        require(pid not in samples and sample["sha256"] == md["deployed_sha256"] and sample["bytes"] == md["deployed_bytes"]
                and sample["exe_link"] == md["deployed_path"], "actual process image binding")
        stat = sample["stat"]
        end = stat.rfind(")")
        require(end > 0 and stat.split(" ", 1)[0] == str(pid), "proc stat PID")
        rest = stat[end + 2:].split()
        require(len(rest) >= 20 and int(rest[19]) > 0, "proc stat start ticks")
        samples[pid] = sample
    events = cli["events"]
    cli_log = archive.get("md-restart/cli-acceptance.log")
    require(cli_log is not None, "missing CLI process log")
    logged_events = [decode(line) for line in cli_log.splitlines() if line.startswith(b"{")]
    require(logged_events == events, "CLI metadata events differ from actual test output")
    require(b"test result: ok. 17 passed; 0 failed; 0 ignored;" in cli_log, "CLI runtime test result")
    require(cli["command"] == ["cargo", "test", "-p", "tickwire-md", "--lib", "server::runtime_tests", "--", "--nocapture"], "unexpected CLI test command")
    def one(name):
        rows = [e for e in events if e.get("event") == name]
        require(len(rows) == 1, f"event count: {name}")
        return rows[0]
    expanded = one("md_cold_restart_verified")
    require(expanded["old_pid"] != expanded["new_pid"] and all(p in samples for p in (expanded["old_pid"], expanded["new_pid"])), "restart actual process samples")
    require(expanded["revision"] == 3 and expanded["instrument_ids"] == IDS and expanded["quota_grants"] == 2, "expanded event result")
    fault = one("md_starting_fault_recovered")
    pre = one("md_starting_fault_preconditions")
    require(fault["pid"] == pre["pid"] and fault["pid"] in samples and fault["depth_live_sequence"] == pre["depth_live_sequence"] > 0, "starting fault process/frontier")
    require(pre["depth_subscriptions"] >= 2 and pre["snapshots"] >= 2 and pre["trade_records"] >= 1
            and pre["old_resource"] != pre["replacement_resource"], "starting fault network observations")
    killed = [e for e in events if e.get("event") == "md_incomplete_restart_rejected"]
    require(len(killed) == 4 and {e["phase"] for e in killed} == PHASES - {"ready"}, "four actual incomplete stages")
    for event in killed:
        require(uint(event["killed_pid"], 32, True) != uint(event["rejected_pid"], 32, True), "rejected child PID reused")
        octets(event["checkpoint"], 32, "killed checkpoint", True)
    histories = {}
    for name, event in (("expanded", expanded), ("starting-fault", fault)):
        prefix = f"md-restart/recovery/{name}"
        actual = list((base / prefix).iterdir())
        require(all(p.is_file() and not p.is_symlink() for p in actual), f"{name}: non-regular CAS entry")
        require({f"{prefix}/{p.name}" for p in actual} == {p for p in archive if p.startswith(prefix + "/")}, f"{name}: unlisted CAS entry")
        chain = history(archive, prefix)
        tail = chain[-1]["value"]
        require(chain[-1]["hash"] == h(event["checkpoint"], "event checkpoint") and tail["phase"] == "ready", f"{name}: event/HEAD mismatch")
        require(tail["config"]["expansion"]["recovery"]["directory"] == event["recovery_directory"], f"{name}: recorded directory binding")
        if name == "expanded":
            require(tail["revision"] == 3 and tail["instrument_ids"] == IDS, "expanded exact final state")
            require(len({e["producer"] for e in chain}) == 2, "expanded producer count")
        else:
            require(len(chain) >= 3 and tail["revision"] == 1 and tail["instrument_ids"] == IDS[:1], "starting fault final state")
            prior = chain[-2]["value"]
            require(prior["phase"] == "starting" and prior["banks"] == tail["banks"], "starting reservation must precede Ready")
            require(chain[0]["banks"][0]["resource"] == bytes(pre["old_resource"])
                    and chain[-1]["banks"][0]["resource"] == bytes(pre["replacement_resource"]), "fault bank transition")
        histories[name] = {"objects": len(chain), "revision": tail["revision"], "head": chain[-1]["hash"]}
    return {"status": "verified", "artifacts": len(report["artifacts"]), "source_inputs": source["file_count"],
            "binaries": 8, "actual_process_samples": len(samples), "killed_stages": 4, "histories": histories,
            "scope": "archived bytes and recorded identities/content only; no network rerun, current launch authority, or performance claim"}


if __name__ == "__main__":
    try:
        require(len(sys.argv) <= 2, "usage: verify-md-restart.py [REPORT.json]")
        print(json.dumps(verify(sys.argv[1] if len(sys.argv) == 2 else None)))
    except (OSError, ValueError, KeyError, TypeError, IndexError, OverflowError) as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        sys.exit(1)
