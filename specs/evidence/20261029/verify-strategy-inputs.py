#!/usr/bin/env python3
"""Read-only verification of the archived strategy-input acceptance package.

Python 3.11+ standard library only. Recomputes small immutable object hashes,
merged-input continuity, account input seals, exact bank/JOIN member bindings and
configuration content. Logs retain the actual process assertions. This does not
replay strategy/account economics, rerun workers, independently authenticate past
kernel observations/exits, submit orders, or establish global Applied. Large ELF
objects are omitted and bound to the recorded frozen binary manifest only.

Usage: python3 verify-strategy-inputs.py [strategy-inputs.json]
"""
import hashlib
import json
import math
from pathlib import Path
import re
import sys


class Checks:
    def __init__(self):
        self.count = 0

    def require(self, condition, message):
        if not condition:
            raise ValueError(message)
        self.count += 1


C = Checks()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw, *, metadata=False):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def unsupported(value):
        raise ValueError(f"non-integer JSON number: {value}")

    def finite(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("nonfinite metadata number")
        return parsed

    return json.loads(raw, object_pairs_hook=unique,
                      parse_float=finite if metadata else unsupported, parse_constant=unsupported)


def serde_bytes(value):
    # Typed replay checkpoints use serde_json struct field order, not config JCS.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def candidate_bytes(value):
    # tickwire-config/src/strict.rs sorts UTF-16 code units, not Unicode scalars.
    # Candidate::parse hashes these bytes directly; it adds no domain prefix.
    def append(v, depth):
        C.require(depth <= 64, "candidate JSON depth")
        if isinstance(v, dict):
            keys = sorted(v, key=lambda key: key.encode("utf-16-be"))
            return b"{" + b",".join(serde_bytes(k) + b":" + append(v[k], depth + 1)
                                     for k in keys) + b"}"
        if isinstance(v, list):
            return b"[" + b",".join(append(item, depth + 1) for item in v) + b"]"
        if type(v) is int:
            C.require(abs(v) <= 9_007_199_254_740_991, "candidate integer bounds")
        else:
            C.require(v is None or type(v) in (str, bool), "candidate scalar type")
        return serde_bytes(v)
    return append(value, 0)


def octets(value, size=None):
    C.require(isinstance(value, list) and all(type(n) is int and 0 <= n <= 255 for n in value),
              "invalid byte array")
    if size is not None:
        C.require(len(value) == size, "byte array length")
    return bytes(value)


def hash_array(value):
    raw = octets(value, 32)
    C.require(any(raw), "zero hash")
    return raw.hex()


def object_matches(raw, obj, label):
    C.require(type(obj["bytes"]) is int and obj["bytes"] > 0, f"{label}: object length")
    C.require(len(raw) == obj["bytes"] and sha(raw) == hash_array(obj["digest"]),
              f"{label}: object bytes/hash")


def artifact_hash(reference):
    C.require(isinstance(reference, str) and re.fullmatch(
        r"artifact://[a-z][a-z0-9-]*/[a-z0-9][a-z0-9/_-]*@sha256:[0-9a-f]{64}", reference),
        "artifact reference")
    return reference.rsplit("@sha256:", 1)[1]


def cbor_value(raw, limit=2 * 1024 * 1024):
    """Bounded canonical CBOR framing; this is not an account reducer."""
    C.require(0 < len(raw) <= limit, "CBOR byte bounds")
    offset = 0
    nodes = 0

    def take(count):
        nonlocal offset
        C.require(count <= len(raw) - offset, "truncated command CBOR")
        value = raw[offset:offset + count]
        offset += count
        return value

    def item(depth):
        nonlocal nodes
        nodes += 1
        C.require(depth <= 32 and nodes <= 1_000_000, "command CBOR bounds")
        head = take(1)[0]
        major, additional = head >> 5, head & 31
        if additional < 24:
            argument = additional
        else:
            C.require(additional in (24, 25, 26, 27), "indefinite/reserved CBOR")
            argument = int.from_bytes(take(1 << (additional - 24)), "big")
            C.require(argument >= (24, 256, 65_536, 4_294_967_296)[additional - 24],
                      "nonminimal command CBOR")
        if major == 0:
            return argument
        if major == 1:
            return -1 - argument
        if major in (2, 3):
            data = take(argument)
            return data if major == 2 else data.decode("utf-8")
        if major == 4:
            C.require(argument <= 16_384, "command array bounds")
            return [item(depth + 1) for _ in range(argument)]
        if major == 6 and argument in (2, 3):
            magnitude = item(depth + 1)
            C.require(isinstance(magnitude, bytes) and 9 <= len(magnitude) <= 16 and
                      magnitude[0] != 0, "command bignum encoding")
            number = int.from_bytes(magnitude, "big")
            return number if argument == 2 else -1 - number
        raise ValueError("unsupported command CBOR major type")

    result = item(0)
    C.require(offset == len(raw), "trailing command CBOR")
    return result


def command_value(raw):
    result = cbor_value(raw, 16_384)
    C.require(isinstance(result, list) and len(result) == 4 and
              result[:2] == ["tickwire.venue-command", 1], "venue command domain/version")
    context = result[2]
    C.require(isinstance(context, list) and len(context) == 4 and
              len(context[0]) == 11 and len(context[1]) == 11,
              "venue command context")
    return result


def bind_command(actual_raw, template_raw, identity):
    actual = command_value(actual_raw)
    template = command_value(template_raw)
    request, strategy, instrument, triggers = actual[2]
    C.require(request[7] == identity["generation"] and request[8] == 6 and
              strategy[0] == identity["logical_strategy"] and
              strategy[1] == octets(identity["instance"], 16), "actual command identity/revision")
    C.require(type(strategy[6]) is int and 0 < strategy[6] < request[9],
              "actual command creation/deadline")
    C.require(isinstance(triggers, list) and len(triggers) == 1 and
              len(triggers[0]) == 7 and triggers[0][4:6] == instrument[:2] and
              triggers[0][6] == strategy[6], "actual market trigger/time binding")
    # CountingStrategy changes exactly created_mono_ns and the market triggers.
    # All other identity, request, instrument and economic command bytes remain.
    actual[2][1][6] = template[2][1][6]
    actual[2][3] = template[2][3]
    C.require(actual == template, "actual command differs from approved template")


def number(raw, offset, size=8, order="little"):
    C.require(0 <= offset <= len(raw) - size, "truncated binary integer")
    return int.from_bytes(raw[offset:offset + size], order)


def canonical(raw, label):
    value = decode(raw)
    C.require(serde_bytes(value) == raw, label + ": canonical typed JSON")
    return value


def bank(raw, normalize=False):
    C.require(len(raw) == 4096 and raw[:8] == b"TWIPC001", "resource header framing")
    blank = raw[:272] + bytes(32) + raw[304:]
    digest = hashlib.sha256(blank).digest()
    C.require(raw[272:304] == digest or (normalize and raw[272:304] == bytes(32)),
              "resource descriptor hash")
    return raw[:272] + digest + raw[304:]


def checkpoint(raw):
    cp = canonical(raw, "merged checkpoint")
    C.require(cp["schema"] == 2, "merged checkpoint version")
    object_matches(octets(cp["business"]), cp["cut"]["business"], "checkpoint business")
    object_matches(serde_bytes(cp["runtime"]), cp["cut"]["runtime"], "checkpoint runtime")
    return cp


def retained(raw, current_raw):
    C.require(68 <= len(raw) <= 16 * 1024 * 1024 and raw[:8] == b"TWINP001",
              "retained input framing")
    first, count = number(raw, 24, order="big"), number(raw, 32, 4, "big")
    C.require(0 < first < (1 << 63) and count <= 1_000_000 and any(raw[8:24]),
              "retained input identity/count")
    offset, anchor, current, rows = 68, None, None, []
    previous = raw[36:68]
    for index in range(count):
        at = offset
        seq = number(raw, at, order="big")
        C.require(seq == first + index, "retained sequence discontinuity")
        kind, size = raw[at + 8], number(raw, at + 9, 4, "big")
        offset = at + 13 + size
        C.require(1 <= kind <= 5 and offset <= len(raw), "retained row bounds")
        payload = raw[at + 13:offset]
        previous = hashlib.sha256(b"tickwire.input-row.v1" + previous + raw[at:offset]).digest()
        if kind != 5:
            continue
        tag, encoded = payload[:8], payload[8:]
        if tag == b"TWM2A001":
            C.require(anchor is None, "duplicate merged anchor")
            anchor = checkpoint(encoded)
            chain = octets(anchor["cut"]["chain"], 32)
            when = anchor["runtime"]["now_ns"]
        elif tag == b"TWM2R001":
            C.require(anchor is not None and len(rows) < 65_536, "merged row before anchor/bounds")
            row = canonical(encoded, "merged row")
            C.require(row["sequence"] == anchor["cut"]["sequence"] + len(rows) + 1 and
                      row["now_ns"] >= when, "merged sequence/time continuity")
            chain = hashlib.sha256(b"tickwire.strategy-input.v2" + chain +
                                   len(encoded).to_bytes(4, "big") + encoded).digest()
            when = row["now_ns"]
            rows.append(row)
            current = None
        elif tag == b"TWM2C001":
            C.require(anchor is not None, "merged completion without anchor")
            cp = checkpoint(encoded)
            C.require(cp["source"] == anchor["source"] and
                      cp["cut"]["sequence"] == anchor["cut"]["sequence"] + len(rows) and
                      octets(cp["cut"]["chain"], 32) == chain, "merged completion frontier")
            current = encoded
        else:
            raise ValueError("unknown retained merged tag")
    C.require(offset == len(raw) and current == current_raw, "retained final current checkpoint")
    # The outer chain has no independently supplied final root. Its exact bytes
    # are named by HandoverSeal.input_digest; no invented final-hash claim.
    return rows


def market_members(raw, header, levels):
    C.require(88 <= len(raw) <= 64 * 1024 * 1024 and raw[:8] == b"TWMR0001",
              "materialized market framing")
    C.require(hashlib.sha256(raw[:-32]).digest() == raw[-32:], "market state trailing hash")
    C.require(number(raw, 8) == number(header, 168) and number(raw, 16) == number(header, 152)
              and number(raw, 24) == number(header, 144) and number(raw, 40) == levels,
              "materialized market bank/capacity")
    count = number(raw, 48, 4)
    C.require(0 < count <= 4096 and raw[52] in (0, 1) and raw[53:56] == bytes(3),
              "materialized market member count/status")
    result, offset = [], 56
    for _ in range(count):
        C.require(offset + 96 <= len(raw) - 32, "truncated materialized member")
        ident, version = number(raw, offset), number(raw, offset + 8)
        bids, asks, states = (number(raw, offset + k, 4) for k in (80, 84, 88))
        C.require(ident > (result[-1][0] if result else 0) and version > 0 and
                  bids <= levels and asks <= levels and 1 <= states <= 4,
                  "materialized market member identity/bounds")
        C.require(raw[offset + 18:offset + 24] == bytes(6) and
                  raw[offset + 92:offset + 96] == bytes(4), "materialized member reserved bytes")
        offset += 96 + states * 512 + (bids + asks) * 16
        result.append([ident, version])
    C.require(offset == len(raw) - 32, "materialized market trailing bytes")
    return result, number(raw, 32)


def verify_join(raw, header, expected, now, cursor, identity):
    C.require(len(raw) >= 20 and raw[:8] == b"TWSC0001", "named original JOIN framing")
    at = 20 + number(raw, 16, 4)
    saved = raw[at:]
    C.require(len(saved) >= 28 and saved[:8] == b"TWSB0001", "saved JOIN framing")
    when, levels, size = number(saved, 8), number(saved, 16), number(saved, 24, 4)
    C.require(when <= now and 0 < levels <= 10000 and 28 + size <= len(saved),
              "saved JOIN time/capacity")
    bootstrap = canonical(saved[28:28 + size], "saved JOIN bootstrap")
    C.require(octets(bootstrap["bank_header"], 4096) == header, "named JOIN uses another bank")
    snapshot_header = bank(octets(bootstrap["snapshot_header"], 4096))
    for lo, hi in [(72, 88), (104, 136), (160, 168), (184, 192)]:
        C.require(snapshot_header[lo:hi] == header[lo:hi], "snapshot producer/namespace/catalog")
    join = octets(bootstrap["join"], 256)
    C.require(join[:16] == octets(identity["instance"], 16) and
              number(join, 16) == identity["logical_strategy"], "JOIN consumer identity")
    for ja, ha, width in [(32, 168, 8), (40, 56, 16), (56, 136, 8), (64, 152, 8),
                         (72, 144, 8), (136, 40, 16), (192, 184, 8), (200, 328, 2)]:
        C.require(join[ja:ja + width] == header[ha:ha + width], "JOIN/header identity")
    members = [[x["id"], x["version"], x["required_states"]] for x in bootstrap["instruments"]]
    C.require(members == expected["members"] and members == sorted(members) and
              len({x[0] for x in members}) == len(members), "JOIN complete member set")
    set_hash = hashlib.sha256(b"".join(i.to_bytes(8, "little") + v.to_bytes(8, "little")
                                     for i, v, _ in members)).digest()
    C.require(join[216:248] == set_hash and number(join, 208) ==
              bootstrap["instrument_set_version"] == expected["instrument_set_version"],
              "JOIN instrument-set digest/version")
    C.require(bootstrap["catalog_hash"] == expected["catalog_hash"], "JOIN catalog hash")
    C.require(number(join, 104) == expected["baseline_sequence"] <= cursor and
              when < number(join, 184), "JOIN baseline/deadline")
    snapshot = saved[28 + size:]
    object_matches(snapshot, expected["snapshot"], "actual JOIN snapshot")
    C.require(join[152:184] == hashlib.sha256(snapshot).digest() and
              snapshot[:4096] == snapshot_header, "JOIN snapshot hash/resource")
    # Full economic snapshot readiness was checked by the live Rust owner. Here
    # hash and member-directory bindings are checked from the retained bytes.
    C.require(len(snapshot) >= 4352 + 128 * len(members), "snapshot directory bounds")
    for index, (ident, version, _) in enumerate(members):
        off = 4352 + index * 128
        C.require(number(snapshot, off) == ident and number(snapshot, off + 8) == version,
                  "snapshot directory member differs from JOIN registry")
    return bootstrap


def verify_final_checks(document, raw_file, binaries, by_pid, events):
    """Bind recorded command results; these records are not kernel attestations."""
    def metadata(path):
        return decode(raw_file(path), metadata=True)
    def value(path):
        return decode(raw_file(path))
    def sibling(name):
        return str(Path(document["app_process"]["log"]).parent / name)
    def zero(value, label):
        C.require(type(value) is int and value == 0, label)
    expected = {b["name"]: b["deployed_sha256"] for b in binaries["binaries"]}
    C.require(len(expected) == len(binaries["binaries"]) == 8, "frozen binary count")
    worker = next(b for b in binaries["binaries"] if b["name"] == "tickwire-counting-worker")
    def unchanged(meta, label):
        C.require(meta["source_manifest_verified"] is True and
                  meta["binary_hashes_before"] == meta["binary_hashes_after"] == expected,
                  label + ": recorded source/binary identity changed")
    def samples(rows, allowed, label):
        C.require(isinstance(rows, list) and len(rows) > 0, label + ": no recorded image sample")
        pids = set()
        for row in rows:
            C.require(type(row["pid"]) is int and row["pid"] > 0 and
                      row["pid"] in allowed and row["pid"] not in pids, label + ": sampled PID binding")
            pids.add(row["pid"])
            C.require(row["exe_link"] == "/memfd:tickwire-executable (deleted)" and
                      row["sha256"] == worker["deployed_sha256"] and row["bytes"] == worker["deployed_bytes"],
                      label + ": recorded memfd image binding")
            C.require(int(row["stat"].split(" ", 1)[0]) == row["pid"], label + ": recorded kernel stat PID")
        return pids
    checks = document["checks"]
    zero(checks["contracts_exit_code"], "design contracts failed")
    gate = metadata(checks["full_rust"]["metadata"])
    zero(checks["full_rust"]["exit_code"], "full Rust declared failure")
    zero(gate["exit_code"], "full Rust process failure")
    C.require("scripts/check-rust.sh" in gate["command"], "wrong full Rust command")
    app = document["app_process"]
    app_meta = metadata(app["metadata"])
    zero(app_meta["exit_code"], "App metadata process failure")
    unchanged(app_meta, "App process")
    C.require(app_meta["environment"]["TICKWIRE_INPUTS_WORKER"] == worker["deployed_path"],
              "App selected another worker")
    all_pids = {e[k] for e in events for k in ("pid", "worker_pid", "old_pid", "new_pid")
                if type(e.get(k)) is int and e[k] > 0}
    sampled = samples(app_meta["live_images"], all_pids, "App process")
    C.require(sampled.intersection(by_pid), "no successful input consumer externally sampled")
    C.require(app["external_image_samples"] == len(sampled), "App external sample count")
    parameter = metadata(checks["parameter_compat"]["metadata"])
    zero(checks["parameter_compat"]["exit_code"], "parameter compatibility declared failure")
    zero(parameter["exit_code"], "parameter compatibility process failure")
    unchanged(parameter, "parameter compatibility")
    C.require(parameter["environment"]["TICKWIRE_PARAMETERS_WORKER"] == worker["deployed_path"],
              "parameter compatibility selected another worker")
    parameter_log = raw_file(sibling("parameter-compat.log")).decode("utf-8")
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
    host_log = raw_file(sibling("host-process.log")).decode("utf-8")
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
    return {"full_rust": True, "parameter_compat_tests": 6, "host_process_tests": 1,
            "spot_account_records": report["account_records"], "app_image_samples": len(sampled)}


def verify(manifest_path):
    base = manifest_path.resolve().parent
    def read(relative, limit=32 * 1024 * 1024):
        C.require(isinstance(relative, str), "evidence path type")
        path = Path(relative)
        C.require(not path.is_absolute() and ".." not in path.parts, "nonlocal evidence path")
        path = base / path
        C.require(path.resolve().is_relative_to(base), "evidence path escapes package")
        C.require(not any(p.is_symlink() for p in [path, *path.parents] if p != base), "symlink evidence")
        C.require(path.is_file() and 0 <= path.stat().st_size <= limit, "evidence file bounds")
        return path.read_bytes()
    document = decode(read(manifest_path.name, 4 * 1024 * 1024))
    C.require(document["schema"] == 1, "evidence schema")
    archive, total = {}, 0
    for path, obj in document["artifacts"].items():
        raw = read(path)
        C.require(len(raw) == obj["bytes"] and sha(raw) == obj["sha256"], "artifact changed: " + path)
        total += len(raw)
        C.require(total <= 128 * 1024 * 1024, "archive exceeds 128 MiB")
        archive[path] = raw
    def raw_file(path):
        C.require(path in archive, "unlisted artifact: " + path)
        return archive[path]
    def value(path):
        return decode(raw_file(path))
    source = document["source_binding"]
    C.require(sha(read(source["manifest"], 4 * 1024 * 1024)) == source["manifest_sha256"],
              "source manifest binding")
    binaries = value(document["build"]["binary_manifest"])
    C.require(binaries["build_exit_code"] == 0 and
              binaries["source_manifest_sha256"] == source["manifest_sha256"], "binary/source build binding")
    workers = [b for b in binaries["binaries"] if b["name"] == "tickwire-counting-worker"]
    C.require(len(workers) == 1, "worker manifest ambiguity")
    worker_hash = workers[0]["deployed_sha256"]
    C.require(re.fullmatch(r"[0-9a-f]{64}", worker_hash) is not None, "worker hash format")
    app = document["app_process"]
    C.require(app["exit_code"] == 0, "actual process tests failed")
    log = raw_file(app["log"]).decode("utf-8")
    C.require("test result: ok. 8 passed; 0 failed; 0 ignored;" in log, "actual eight-test summary")
    names = ["actual_scope_only_consumption_matches_one_account_and_external_topic",
             "actual_scope_and_parameters_share_one_challenge_and_current_checkpoint",
             "input_scope_rejects_extra_sets_and_wrong_owner_before_worker_start",
             "gates::actual_queued_intent_waits_for_scope_and_cannot_steal_internal_query",
             "gates::scope_timeout_reaps_worker_and_keeps_queued_intent_closed",
             "gates::identical_baseline_is_idempotent_but_changed_baseline_closes_after_proof",
             "lifecycle::scope_proof_rejects_arm_and_direct_commit_before_host_dispatch",
             "lifecycle::multi_cannot_activate_unscoped_shadow_past_scoped_active"]
    for name in names:
        C.require(f"test {name} ... ok" in log, "missing passed process test: " + name)
    events = [decode(line) for line in log.splitlines() if line.startswith("{")]
    consumed = [e for e in events if e.get("case") == "input_scope_consumed"]
    fixtures = {f["original_root"]: f for f in app["fixtures"]}
    C.require(len(consumed) == len(fixtures) == len(app["fixtures"]) == 6, "fixture cardinality")
    C.require(set(fixtures) == {e["root"] for e in consumed}, "fixture/log roots")
    challenges, by_pid, by_command = set(), {}, {}
    total_rows = 0
    for row in consumed:
        f = fixtures[row["root"]]
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
        C.require(named(dep[11]) == raw_file(f["original_join"]), "kind 11 original JOIN binding")
        total_rows += len(retained(named(seal["input_digest"], seal["input_bytes"]), cp_raw))
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
    parameters = [e for e in events if e.get("case") == "parameter_consumed"]
    C.require(len(parameters) == 1 and parameters[0]["pid"] in by_pid, "combined parameter/input case")
    param = parameters[0]
    proof = by_pid[param["pid"]][0]
    C.require(param["challenge"] == proof["challenge"] and param["checkpoint"] == proof["checkpoint"] and
              param["current_cut"] == proof["cut"] and param["global_revision"] == 41 and param["private_revision"] == 6,
              "combined consumers share one exact challenge/cut")
    def one(case):
        found = [e for e in events if e.get("case") == case]
        C.require(len(found) == 1, "scenario event cardinality: " + case)
        return found[0]
    pending = one("input_pending_gate")
    C.require(pending["pid"] in by_pid and pending["effect_before_query"] == 1 and
              pending["pending_delivered"] is False and pending["fresh_proof_then_same_intent"] is True and
              pending["no_gateway_submission"] is True and
              hash_array(pending["intent_digest"]) == by_pid[pending["pid"]][1], "pending query actual intent gate")
    expired = one("input_query_expired")
    C.require(expired["pid"] not in by_pid and expired["delivered"] is False and
              expired["inputs_proof"] is False and expired["parameters_proof"] is False and expired["reaped"] is True and
              0 < expired["earlier_timeout_ns"] < expired["parameter_timeout_ns"], "expired query closed/reaped")
    changed = one("input_baseline_changed")
    C.require(changed["pid"] in by_pid and changed["original"] == by_pid[changed["pid"]][0]["account"]["baseline"] and
              changed["replacement"] != changed["original"] and changed["same_scope"] is True and
              changed["continued_account_position"] > 0 and changed["reaped"] is True and changed["delivered"] is False,
              "logged baseline mutation closes scoped owner")
    mutation = one("input_mutations_rejected")
    C.require(mutation["pid"] in by_pid and mutation["valid_arm"] is True and mutation["valid_commit"] is True and
              mutation["arm_dispatched"] is False and mutation["commit_dispatched"] is False and
              mutation["continued_market_sequence"] > 0, "scope blocks later market mutation")
    multi = one("input_multi_handover_closed")
    C.require(multi["old_pid"] in by_pid and multi["old_pid"] != multi["new_pid"] and
              multi["old_instance"] == by_pid[multi["old_pid"]][0]["source"]["instance"] and
              multi["new_instance"] != multi["old_instance"] and multi["quiesced"] is False and
              multi["activated"] is False and multi["delivered"] is False, "scope blocks Multi successor")
    C.require(document["scope"]["gateway_submission"] is False and document["scope"]["global_applied"] is False,
              "evidence scope overclaim")
    final_checks = None
    if not document.get("development_only", False):
        final_checks = verify_final_checks(document, raw_file, binaries, by_pid, events)
    return {"result": "PASS", "final_checks": final_checks, "evidence_mode": "development" if document.get("development_only") else "archived",
            "checks": C.count, "archived_files": len(archive), "input_proofs": len(consumed),
            "independent_challenges": len(challenges), "merged_rows": total_rows,
            "gateway_submission": False, "global_applied": False,
            "boundary": "Archived byte bindings and logged process outcomes; no economic replay, kernel reauthentication, or ELF rehash."}


if __name__ == "__main__":
    try:
        path = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).with_name("strategy-inputs.json")
        if len(sys.argv) > 2:
            raise ValueError("usage: verify-strategy-inputs.py [manifest.json]")
        print(json.dumps(verify(path), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, IndexError, RecursionError, OverflowError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
