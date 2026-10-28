#!/usr/bin/env python3
"""Verify the adjacent historical evidence package without reading runtime paths.

Requires Python 3.11+ and cryptography (``python3 -m pip install cryptography``).
Checks archived bytes, signatures and recorded bindings; does not rerun processes,
read /tmp, validate today's source tree, or reconstruct the omitted recovery CAS.
"""

import hashlib
import json
import re
from pathlib import Path
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(raw, object_pairs_hook=unique)


def octets(value, length, label):
    require(isinstance(value, list) and len(value) == length, f"{label} width")
    require(all(type(x) is int and 0 <= x <= 255 for x in value), f"{label} bytes")
    return bytes(value)


def compact(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def json_lines(raw):
    return [decode(line) for line in raw.splitlines() if line.startswith(b"{")]


def record_prefix(raw, baseline, sequence):
    # Only the fixed envelope is read here. Economic CBOR replay and complete
    # canonical validation were executed by the archived independent Rust tool.
    prefix = b"\x85\x77tickwire.account-record\x01\x58\x20" + baseline
    require(raw.startswith(prefix) and len(raw) <= 60 * 1024, "account record envelope")
    at = len(prefix)
    first = raw[at]
    require(first >> 5 == 0, "account record sequence type")
    info = first & 31
    if info < 24:
        number, end = info, at + 1
    else:
        require(info in (24, 25, 26, 27), "account record integer encoding")
        width = 1 << (info - 24)
        require(at + 1 + width < len(raw), "truncated account sequence")
        number = int.from_bytes(raw[at + 1:at + 1 + width], "big")
        minimum = {1: 24, 2: 256, 4: 65536, 8: 4294967296}[width]
        require(number >= minimum, "noncanonical account sequence")
        end = at + 1 + width
    require(number == sequence and end < len(raw), "account prefix sequence")


def account_prefixes(baseline_raw, records):
    baseline = hashlib.sha256(baseline_raw).digest()
    chain = hashlib.sha256(b"tickwire.account-input-seal.v1" + baseline).digest()
    prefixes = {0: chain.hex()}
    for sequence, raw in enumerate(records, 1):
        record_prefix(raw, baseline, sequence)
        chain = hashlib.sha256(b"tickwire.account-input-seal.step.v1" + chain + sequence.to_bytes(8, "little") + hashlib.sha256(raw).digest()).digest()
        prefixes[sequence] = chain.hex()
    return prefixes


def input_history(raw, report, events):
    """Read the frozen TWAL/TWAI envelope, not replay account economic effects."""
    def number(data, start, size):
        require(start + size <= len(data), "short input frame field")
        return int.from_bytes(data[start:start + size], "little")

    def crc32c(data):
        crc = 0xffffffff
        for byte in data:
            crc ^= byte
            for _ in range(8):
                crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
        return crc ^ 0xffffffff

    at, previous, records, gaps, begins, quarantines = 0, bytes(32), [], [], [], 0
    while at < len(raw):
        total = number(raw, at + 12, 4)
        require(176 <= total <= 1048576 and total % 8 == 0 and at + total <= len(raw), "input frame length")
        frame = raw[at:at + total]
        require(frame[:12] == b"TWAL0001\x01\x00\x88\x00" and frame[-4:] == b"TEND", "input frame version")
        require(number(frame, total - 8, 4) == total and number(frame, 18, 2) == 0, "input frame trailer or flags")
        size = number(frame, 20, 4)
        require(0 <= total - 176 - size < 8, "input frame payload bounds")
        payload = frame[136:136 + size]
        header = bytearray(frame[:136])
        header[128:132] = bytes(4)
        require(crc32c(header) == number(frame, 128, 4) and crc32c(payload) == number(frame, 132, 4), "input frame CRC")
        require(frame[96:128] == previous and not any(frame[136 + size:total - 40]), "input frame chain or padding")
        previous = hashlib.sha256(frame[:total - 40] + frame[total - 8:]).digest()
        require(previous == frame[total - 40:total - 8], "input frame SHA")
        require(frame[24:40].hex() == report["input_stream"], "input stream identity")
        if number(frame, 16, 2) == 2:
            require(payload[:8] == b"TWAI0001" and len(payload) >= 9, "input record version")
            sequence, tag = number(frame, 72, 8), payload[8]
            require(sequence == len(records) + 1, "input record order")
            records.append(tag)
            if tag == 4:
                require(len(payload) == 26, "gap record size")
                gaps.append({"record": sequence, "epoch": number(payload, 9, 8), "generation": number(payload, 17, 8), "reason": payload[25]})
            elif tag == 10:
                require(len(payload) == 25, "reconciliation record size")
                begins.append({"record": sequence, "epoch": number(payload, 9, 8), "generation": number(payload, 17, 8)})
            elif tag == 3:
                decision = 39 + number(payload, 35, 4)
                require(decision < len(payload) and payload[decision] in (1, 2, 3), "account event decision")
                quarantines += payload[decision] == 2
        at += total
    require(len(records) == report["input_records"], "input WAL record count")
    require(quarantines + len(gaps) == report["input_quarantines_and_gaps"], "cumulative gap/quarantine count")
    require(quarantines == 0 and len(gaps) <= 1, "unexpected quarantined event or additional source gap")
    for gap in gaps:
        require(gap == {"record": 2, "epoch": 1, "generation": 2, "reason": 4}, "unexpected source gap")
        resumed = {"record": 3, "epoch": 2, "generation": 2}
        require(resumed in begins and records.index(2) + 1 > resumed["record"], "source gap not followed by reconciliation before command registration")
        reconciled = [i for i, e in enumerate(events) if e.get("event") == "account_reconciled" and e.get("connection_epoch") == 2 and e.get("gap_generation") == 2]
        submitted = [i for i, e in enumerate(events) if e.get("event") == "hosted_submission"]
        require(reconciled and submitted and reconciled[0] < submitted[0], "missing actual reconciliation before command submission")
    return {"records": len(records), "gaps": gaps, "quarantined_events": quarantines, "scope": "initial Clock gap, if present, followed by actual reconciliation; not a claim of uninterrupted input"}


def business_report(report, result, fixture):
    require(report["frozen"] is True and report["format"] == "tickwire.offline-replay.v1", "business replay not frozen")
    require(report["account_records"] == result["mirror_records_verified"] == result["worker_report_position_verified"], "business report frontier")
    require(report["pending_orders"] == [] and report["input_facts_not_in_account"] == 0, "unclosed business facts")
    require(report["discarded_tail_bytes"] == {"account": 0, "input": 0}, "discarded business bytes")
    require(len(report["orders"]) == 1, "expected one actual Spot order")
    order = report["orders"][0]
    require(order["confirmed"] and order["terminal"] and order["fee_reconciled"] and not order["unknown"] and not order["aborted"], "Spot order or fees not closed")
    require(order["filled_quantity"] == order["original_quantity"] and int(order["remaining_quantity"]["atoms"]) == 0, "Spot fill quantity")
    require(int(order["filled_quantity"]["atoms"]) > 0, "missing actual fill")
    posts = [x for x in fixture if x.get("method") == "POST" and x.get("path") == "/api/v3/order"]
    require(len(posts) == 1 and posts[0]["status"] == 200, "actual order submission count")
    require(any(x.get("path") == "/api/v3/myTrades" and x.get("status") == 200 for x in fixture), "actual fee reconciliation query")


def verify_app(app, value, archive, names, source):
    require(app["status"] == "verified" and app["passed"] and app["run_exit_code"] == 0, "App observation acceptance failed")
    files = app["files"]
    metadata = value(files["metadata.json"])
    require({k: v for k, v in app.items() if k != "files"} == {k: v for k, v in metadata.items() if k != "artifacts"}, "App metadata differs")
    for name, expected in metadata["artifact_sha256"].items():
        require(digest(archive[files[name]]) == expected, f"App original artifact binding: {name}")
    require(set(metadata["binaries"]) == set(names), "App fixed binary set")
    for name, row in metadata["binaries"].items():
        require(row["sha256"] == names[name]["deployed_sha256"], f"App fixed binary identity: {name}")
    image = names["tickwire-strategy-worker"]
    before, after = app["worker_before"], app["worker_after"]
    for sample in [before, after]:
        require(sample["image_sha256"] == image["deployed_sha256"] and sample["image_bytes"] == image["deployed_bytes"], "App actual image sample")
        require(sample["exe"] == "/memfd:tickwire-executable (deleted)" and sample["pid"] > 0 and sample["start_ticks"] > 0, "App memfd or process sample")
    for key in ["pid", "start_ticks", "image_sha256", "image_bytes", "uid", "gid"]:
        require(before[key] == after[key], f"App worker changed: {key}")
    require(before["sampled_mono_ns"] <= after["sampled_mono_ns"], "App process sample order")
    session = app["session"]
    require(value(files["initial-session.json"]) == session and session["session_number"] == 1, "initial App session")
    config = app["observation_config"]
    require(list(bytes.fromhex(image["deployed_sha256"])) in config["allowed_build_hashes"], "App build capability admission")
    require(config["owner_uid"] == before["uid"] and config["recover"] is False, "App admission owner")
    report = value(files["account-report.json"])
    result = value(files["run-result.json"])
    require(report == app["account_report"] and result == app["business"], "App business metadata")
    business_report(report, result, json_lines(archive[files["fixture.jsonl"]]))
    mirror = app["mirror_before_stop"]
    require(mirror["verified_from_actual_store_copy"] and mirror["prefix"]["sequence"] == report["account_records"], "mirror was not durably caught up before stop")
    require(mirror["prefix"]["writer_epoch"] == session["binding"]["writer_epoch"], "mirror writer epoch")
    require(any(octets(mirror["prefix"]["dependency_root"], 32, "mirror dependency root")) and any(octets(mirror["prefix"]["hash"], 32, "mirror prefix hash")), "missing actual mirror proof")
    input_audit = input_history(archive[files["input-history.wal"]], report, json_lines(archive[files["exec.stderr.log"]]))
    baseline = archive[files["business-baseline.cbor"]]
    require(digest(baseline) == report["baseline_sha256"], "App baseline binding")
    record_names = sorted(name for name in files if name.startswith("business-records/"))
    require(record_names == [f"business-records/{n:020}.cbor" for n in range(1, report["account_records"] + 1)], "App account record set")
    prefixes = account_prefixes(baseline, [archive[files[name]] for name in record_names])
    audit = value(files["closure-audit.json"])
    require(len(audit["observations"]) == 2 and len(app["observations"]) == 2, "App successful query count")

    def outcome(row):
        name = "outcomes/" + Path(row["outcome"]).name
        out = value(files[name])
        request = out["request"]
        require(out["schema"] == request["schema"] == 1, "outcome schema")
        require(octets(request["request_id"], 16, "request ID").hex() == row["request_id"], "outcome request ID")
        require(octets(out["request_digest"], 32, "request digest").hex() == digest(compact(request)), "canonical request digest")
        return out

    outcomes = []
    for row, checked in zip(app["observations"], audit["observations"]):
        out = outcome(row)
        outcomes.append(out)
        request, fact = out["request"], out["observation"]
        require(out["status"] == "observed" and not out["historical"] and fact is not None, "actual fresh outcome")
        require(request["binding"] == session["binding"] and request["session_id"] == session["session_id"], "fresh session or live binding")
        target, process = request["target"], fact["process"]
        require(process["logical_strategy_id"] == target["logical_strategy_id"] and process["instance"] == target["instance"] and process["generation"] == target["generation"], "fresh target identity")
        require(process["role"] == 3 and process["pid"] == before["pid"] == row["pid"], "fresh actual worker")
        require(process["start_ticks"] == before["start_ticks"] and process["image_bytes"] == before["image_bytes"], "fresh actual executable lifetime")
        require(octets(process["build_hash"], 32, "build hash").hex() == before["image_sha256"], "fresh actual executable hash")
        require(process["uid"] == before["uid"] and process["gid"] == before["gid"], "fresh credentials")
        require(process["connected_epoch"] == fact["control_epoch"] and fact["control_epoch"] > 0, "fresh authenticated epoch")
        sample = row["sample"]
        for key in ["pid", "start_ticks", "image_sha256", "image_bytes", "uid", "gid"]:
            require(sample[key] == before[key], f"fresh independent process sample: {key}")
        require(fact["started_mono_ns"] <= fact["observed_mono_ns"] <= out["completed_mono_ns"] <= sample["sampled_mono_ns"] < request["valid_until_mono_ns"], "fresh original deadline")
        challenge = octets(fact["challenge"], 16, "challenge")
        require(any(challenge) and challenge.hex() == row["challenge"], "fresh new challenge")
        require(fact["ready_sequence"] == row["ready_sequence"] > 0, "fresh READY sequence")
        require(fact["account_position"] == row["account_position"] >= fact["report_sequence_floor"] == row["floor"], "fresh report lower bound")
        require(fact["report_ring_sequence"] >= fact["account_position"] + 1, "fresh report ring cursor")
        require(not request["require_resources"] and fact["resources"] is None, "legacy resource coverage")
        objects = fact["objects"]
        hashes = [octets(root["digest"], 32, "object digest") for root in objects]
        require(0 < len(objects) == row["objects"] <= 1024 and hashes == sorted(set(hashes)), "fresh objects must be ordered and unique")
        require(all(any(h) and 0 < root["bytes"] <= 64 * 1024 * 1024 for h, root in zip(hashes, objects)), "fresh object budget")
        require(sum(root["bytes"] for root in objects) <= 64 * 1024 * 1024, "fresh closure budget")
        require(fact["checkpoint"] in objects and fact["input_seal"] in objects, "fresh READY roots")
        verified = row["semantic_verification"]
        require(checked["request_id"] == row["request_id"] and checked["objects"] == objects and checked["verified"] == verified, "online closure audit binding")
        require(verified["closure_verified"] and verified["independent_export_prefix_verified"], "missing online semantic verification")
        require(verified["request_id"] == row["request_id"] and verified["ready_sequence"] == fact["ready_sequence"] and verified["account_position"] == fact["account_position"], "online verifier query binding")
        require(verified["bytes"] == sum(root["bytes"] for root in objects), "online closure byte total")
        for field in ["checkpoint", "input_seal"]:
            require(bytes(fact[field]["digest"]).hex() == verified[field], f"online {field} binding")
        expected_prefix = prefixes[fact["account_position"]]
        require(bytes(fact["account_input_digest"]).hex() == verified["account_input_digest"] == expected_prefix, "independent archived account input prefix")
    first, second = [out["observation"] for out in outcomes]
    require(first["challenge"] != second["challenge"] and first["ready_sequence"] < second["ready_sequence"], "reused fresh challenge or READY")
    require(first["process"] == second["process"], "two observations changed actual worker")
    require(first["observed_mono_ns"] <= second["started_mono_ns"] and first["account_position"] <= second["account_position"], "successive fresh observations regressed")

    negative = app["negative_results"]
    require([row["status"] for row in negative] == ["rejected", "rejected", "expired", "unsupported"], "negative request set")
    failures = [outcome(row) for row in negative]
    for row, out in zip(negative, failures):
        require(out["status"] == row["status"] and not out["historical"] and out["observation"] is None, "negative request produced a fact")
    require(failures[0]["request"]["target"] != outcomes[0]["request"]["target"], "wrong-target negative missing")
    require(failures[1]["request"]["session_id"] != session["session_id"], "wrong-session negative missing")
    require(failures[2]["request"]["valid_until_mono_ns"] < failures[2]["completed_mono_ns"], "expired-request negative missing")
    require(failures[3]["request"]["require_resources"] is True, "missing-resource negative missing")
    require(app["identical_request_no_new_result"] and app["conflicting_completed_id_rejected"], "idempotency or conflict failed")
    duplicate = app["duplicate_identical"]
    original = archive[files["outcomes/" + Path(app["observations"][1]["outcome"]).name]]
    require(duplicate["before_sha256"] == duplicate["after_sha256"] == digest(original), "duplicate request changed result bytes")
    conflict = value(files["outcomes/conflict.json"])
    require(conflict["status"] == "rejected" and conflict["observation"] is None and not conflict["historical"], "conflicting ID not rejected")
    require(conflict["request"]["request_id"] == outcomes[1]["request"]["request_id"] and conflict["request"] != outcomes[1]["request"], "conflict is not same ID different content")
    require(bytes(conflict["request_digest"]).hex() == digest(compact(conflict["request"])), "conflicting request digest")
    recovery = app["recovery"]
    require(recovery["exit_code"] == 0 and recovery["unfinished_request_interrupted_without_redispatch"] and recovery["original_worker_exited"], "App recovery failed")
    require(recovery["session"] == value(files["session.json"]) and recovery["session"]["session_number"] == 2 and recovery["session"]["session_id"] != session["session_id"], "recovered session reused identity")
    historical = value(files["outcomes/historical.json"])
    require(historical == {**outcomes[0], "historical": True}, "historical result changed original fact or time")
    interrupted = value(files["outcomes/interrupted.json"])
    require(interrupted["request"] == app["unfinished_request"] and interrupted["historical"] and interrupted["status"] == "interrupted" and interrupted["observation"] is None, "unfinished request redispatched or fabricated")
    require(bytes(interrupted["request_digest"]).hex() == digest(compact(interrupted["request"])), "interrupted request digest")
    return {"observed": 2, "negative": 4, "historical": 1, "interrupted": 1, "account_prefixes": 2, "input_history": input_audit}


def verify(base=None):
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ImportError as error:
        raise ValueError("cryptography is required: python3 -m pip install cryptography") from error

    base = Path(base).resolve() if base is not None else Path(__file__).resolve().parent

    def read(relative):
        path = Path(relative)
        require(not path.is_absolute() and ".." not in path.parts, "nonlocal evidence path")
        path = base / path
        require(path.resolve().is_relative_to(base), "evidence path leaves package")
        require(not any(p.is_symlink() for p in [path, *path.parents] if p != base), "symlink evidence")
        require(path.is_file() and path.stat().st_size <= 4 * 1024 * 1024, "evidence file bounds")
        return path.read_bytes()

    document = decode(read("runtime-observation-acceptance.json"))
    require(document["schema"] == 1, "unsupported evidence schema")
    verifier = document["offline_verifier"]
    require(verifier["file"] == "verify-runtime-observation-acceptance.py" and digest(read(verifier["file"])) == verifier["sha256"], "offline verifier identity")
    archive = {}
    for name, expected in document["artifacts"].items():
        require(name.startswith("runtime-observation-acceptance/"), "artifact outside bundle")
        raw = read(name)
        require(len(raw) == expected["bytes"] and digest(raw) == expected["sha256"], f"artifact digest: {name}")
        archive[name] = raw
    listed = set()
    for path in (base / "runtime-observation-acceptance").rglob("*"):
        require(not path.is_symlink(), "symlink in bundle")
        if path.is_file():
            listed.add(path.relative_to(base).as_posix())
    require(listed == set(archive), "unlisted or missing bundle file")

    def value(reference):
        return decode(archive[reference])

    source = document["source_binding"]
    require(source["manifest"] == "runtime-observation-source-sha256.json", "different source manifest")
    raw = read(source["manifest"])
    require(digest(raw) == source["manifest_sha256"], "historical source manifest digest")
    manifest = decode(raw)
    require(manifest["file_count"] == len(manifest["files"]) == source["file_count"], "source input count")
    require(manifest["git_head"] == source["git_head"], "source base revision")
    for reference in source["verification_logs"]:
        require(archive[reference] == f"UNCHANGED: {source['file_count']} build inputs\n".encode(), "source verification log")

    gate = document["final_rust_gate"]
    gate_meta = value(gate["metadata"])
    gate_log = archive[gate["log"]]
    require(gate_meta["exit_code"] == 0 and b"test result: FAILED" not in gate_log and b"error:" not in gate_log and b"Finished" in gate_log, "final full Rust gate failed")
    gate_counts = [tuple(map(int, match)) for match in re.findall(rb"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", gate_log)]
    require(gate["reported_batches"]["count"] == len(gate_counts) > 0, "Rust gate reported batch count")
    for field, column in [("passed", 0), ("failed", 1), ("ignored", 2)]:
        require(gate["reported_batches"][field] == sum(row[column] for row in gate_counts), "Rust gate reported batch totals")
    require(gate["reported_batches"]["failed"] == 0, "Rust gate contained failures")
    build = document["build"]
    binaries = value(build["binary_manifest"])
    require(build["exit_code"] == binaries["build_exit_code"] == 0, "build failed")
    require(binaries["source_manifest_sha256"] == source["manifest_sha256"], "binary source binding")
    require(len(binaries["binaries"]) == build["binary_count"] == 8, "binary count")
    names = {row["name"]: row for row in binaries["binaries"]}
    require(set(names) == {"tickwire-lab", "tickwire-app", "tickwire-md", "tickwire-strategy-worker", "tickwire-replica-receiver", "tickwire-lifecycle", "tickwire-counting-v1-worker", "tickwire-counting-worker"}, "fixed binary name set")
    worker = names["tickwire-counting-worker"]

    cases = document["scenarios"]
    require({case["scenario"] for case in cases} == {"spot-handover-custom", "spot-rebuild-custom"} and len(cases) == 2, "scenario set")
    signatures = 0
    samples = 0
    for case in cases:
        files = case["files"]
        metadata = value(files["metadata.json"])
        require(metadata["run_exit_code"] == metadata["generate_exit_code"] == case["exit_code"] == 0, "scenario command failed")
        require(metadata["source_manifest_verified"] and metadata["binary_hashes_unchanged"], "scenario changed inputs")
        require(metadata["generate_command"] == case["generate_command"] and metadata["run_command"] == case["run_command"] and case["run_command"][0] == names["tickwire-lab"]["deployed_path"], "scenario executed command binding")
        for stage in ["before", "after"]:
            require(archive[files[f"source-{stage}.log"]] == f"UNCHANGED: {source['file_count']} build inputs\n".encode(), "scenario source verification")
        expected_binaries = {name: row["deployed_sha256"] for name, row in names.items()}
        require(metadata["binary_hashes_before"] == metadata["binary_hashes_after"] == expected_binaries, "scenario binary identity changed")
        result = value(files["run-result.json"])
        require(result == metadata["result"], "result differs from run metadata")
        proof = result["preflight"]
        for field in ["account_fill_verified", "mirror_records_verified", "worker_report_position_verified"]:
            require(result[field] == case[field], f"scenario result: {field}")
        require(result["account_fill_verified"] is True and proof["action_remains_consumed"] is True, "business verification failed")
        for field in ["target_pid", "rollback_pid", "target_final_sequence"]:
            require(proof[field] == case[field], f"preflight binding: {field}")
        if case["scenario"] == "spot-rebuild-custom":
            require(proof["capture_pid"] == proof["target_pid"] == case["capture_pid"], "rebuild changed PID")
            require(proof["prefix_verified"] and proof["rollback_exited"], "rebuild prefix or probe failed")
        report = value(files["account-report.json"])
        business_report(report, result, json_lines(archive[files["fixture.jsonl"]]))
        input_history(archive[files["export/input/segment-00000000000000000001.wal"]], report, json_lines(archive[files["logs/exec.stderr.log"]]))
        baseline = archive[files["export/baseline.cbor"]]
        require(digest(baseline) == report["baseline_sha256"], "scenario baseline digest")
        index = json_lines(archive[files["export/records.jsonl"]])
        require(len(index) == report["account_records"], "scenario account record count")
        records = []
        for sequence, row in enumerate(index, 1):
            require(row["sequence"] == sequence and row["file"] == f"records/{sequence:020}.cbor", "scenario account export order")
            raw = archive[files["export/" + row["file"]]]
            require(len(raw) == row["length"] and digest(raw) == row["sha256"], "scenario account export bytes")
            records.append(raw)
        account_prefixes(baseline, records)
        require(any(row["kind"] == "fees_reconciled" for row in index), "missing fee-close account record")
        coordinator = value(files["logs/coordinator.stdout.log"])
        require(coordinator == case["coordinator"] and coordinator["stage"] == "active_all" and coordinator["pending"] == 0, "coordinator incomplete")
        exits = [decode(line) for line in archive[files["logs/exec.stderr.log"]].splitlines() if line.startswith(b"{")]
        require(case["old_worker_exit"] in exits and case["old_worker_exit"]["exit_status"] == "exit status: 0", "missing successful old process exit")

        sample = value(case["memfd_sample"]["file"])["images"]
        require(len(sample) == 1, "memfd sample count")
        image = sample[0]
        require(image["pid"] == proof["target_pid"] and image["exe_link"] == "/memfd:tickwire-executable (deleted)", "memfd PID binding")
        require(image["sha256"] == worker["deployed_sha256"] and image["bytes"] == worker["deployed_bytes"], "memfd binary binding")
        require(int(image["stat"].split(" ", 1)[0]) == image["pid"], "kernel stat PID binding")
        samples += 1

        keys = value(files["handover/keys.json"])
        key = Ed25519PublicKey.from_public_bytes(bytes(keys["exec"]))
        receipts = case["signed_receipts"]
        require(len(receipts) == 3 and {r["phase"] for r in receipts} == {"prepared", "committed", "released"}, "receipt phase set")
        operation = None
        applied = []
        for receipt in receipts:
            signed = value(receipt["file"])
            payload = bytes(signed["payload"])
            phase = receipt["phase"]
            domain = "tickwire.lifecycle.applied.v1"
            if phase == "prepared":
                family = "replay" if case["scenario"] == "spot-handover-custom" else "rebuild"
                domain = f"tickwire.lifecycle.{family}-prepared.v1"
            require(receipt["signature_domain"] == domain and receipt["signer_public_key"] == "exec", "receipt signature binding")
            require(digest(payload) == receipt["payload_sha256"], "receipt payload digest")
            key.verify(bytes(signed["signature"]), domain.encode() + payload)
            body = decode(payload)
            require(body.get("phase", "prepared") == phase, "receipt phase mismatch")
            require(body["issued_ns"] == receipt["issued_ns"] and body["valid_until_ns"] == receipt["valid_until_ns"] and body["issued_ns"] < body["valid_until_ns"], "receipt original deadline")
            if phase == "prepared":
                operation = body["operation"]
            else:
                require(body["handover_sequence"] == receipt["handover_sequence"] == 4, "handover sequence")
                applied.append(body)
            signatures += 1
        require(bytes(operation["operation_id"]).hex() == coordinator["operation_id"], "coordinator operation identity")
        for receipt in applied:
            require(receipt["operation_id"] == operation["operation_id"] and receipt["operation_revision"] == operation["operation_revision"], "applied receipt operation identity")

    host = document["host_process_tests"]
    metadata = value(host["files"]["host/metadata.json"])
    require(host["exit_code"] == metadata["exit_code"] == 0 and metadata["source_manifest_verified"] and metadata["worker_unchanged"], "Host process run failed")
    require(host["worker_sha256"] == metadata["worker_sha256"] == metadata["worker_sha256_after"] == worker["deployed_sha256"], "Host worker identity")
    require(metadata["environment"]["TICKWIRE_CAPTURE_WORKER"] == worker["deployed_path"], "Host executed different binary path")
    log = archive[host["files"]["host/process.log"]]
    require(host["test_count"] == 6 and b"test result: ok. 6 passed; 0 failed; 0 ignored;" in log, "Host test counts")
    events = json_lines(log)
    fresh = {row["case"]: row for row in events if row.get("case") in {"fresh_minimum_report_floor", "fresh_timeout_preserves_worker", "slow_report_control_reader"}}
    require(list(fresh.values()) == host["fresh_cases"] and len(fresh) == 3, "Host actual fresh process cases")
    floor = fresh["fresh_minimum_report_floor"]
    require(floor["first_ready"] < floor["second_ready"] and floor["floor"] == 0 and floor["actual"] == 2 and floor["continued"] == 3 and floor["same_image"] and floor["resources"] is None, "Host minimum floor and subsequent callbacks")
    timeout = fresh["fresh_timeout_preserves_worker"]
    require(all(timeout[key] for key in ["deadline_rejected", "missing_resources_rejected", "new_query_succeeded", "cancel_busy_until_actual_reply"]) and timeout["after_cancel_account_position"] == 2, "Host timeout and cancel isolation")
    pressure = fresh["slow_report_control_reader"]
    require(pressure["published_accounts"] == pressure["durable_account_position"] == 176 and pressure["queued_query_actual"] >= 0 and pressure["same_live_worker"] and pressure["market_callbacks_continued"] and pressure["final_fresh"] == "rejected_typed_retained_input_capacity", "Host slow reader or typed capacity boundary")
    require(len({row["pid"] for row in fresh.values()}) == 3, "Host process cases must own distinct children")
    retired = {row["worker_pid"] for row in events if row.get("event") == "strategy_retired" and row.get("exit_status") == "exit status: 0"}
    require(all(row["pid"] in retired for row in fresh.values()), "Host normal retirement missing")
    result = value(host["files"]["host/rebuild-result.json"])
    require(result["pid"] == host["rebuild_pid"] and result["probe_pid"] == host["probe_pid"], "Host rebuild PID")
    require(bytes(result["worker_sha256"]).hex() == worker["deployed_sha256"], "Host rebuild binary")
    require(result["C"]["sequence"] == host["installed_sequence"] and result["D"]["sequence"] == host["continued_sequence"], "Host continued input prefix")
    development = document["development_regression"]
    summary = value(development["summary"])
    red = summary["valid_red"]
    red_binary = value(development["logs"]["runtime-observation-process-binary.json"])
    require(red_binary["sha256"] == red["worker_sha256"] and red_binary["worker"] == red["worker"], "development Red executable binding")
    green_binary = value(development["logs"]["runtime-observation-process-binary3.json"])
    require(green_binary == summary["binary"] and green_binary["deployed_sha256"] != red_binary["sha256"], "development Green executable binding")
    red_log = archive[development["logs"][red["log"]]]
    require(b"Transport(Timeout)" in red_log and b"at report 168" in red_log and b"0 passed; 1 failed" in red_log, "actual blocking-send behavior Red")
    require(red["seconds"] < red["query_deadline_seconds"] < red["session_lease_seconds"], "backpressure Red was an expired query")
    unsupported = archive[development["logs"]["runtime-observation-process-red.log"]]
    require(b"ObservationUnsupported" in unsupported and b"0 passed; 1 failed" in unsupported, "initial actual mode-2 behavior Red missing")
    green = archive[development["logs"]["runtime-observation-process-green4.log"]]
    require(b"3 passed; 0 failed" in green, "development process Green")
    require(summary["limits"]["typed_verifier_unchanged"] == 16 * 1024 * 1024, "production typed budget changed for fixture")
    for row in summary["logs"]:
        name = Path(row["path"]).name
        require(digest(archive[development["logs"][name]]) == row["sha256"], "development log hash")
    locks = document["lock_regression"]["files"]
    coverage = value(document["lock_regression"]["coverage"])
    require(len(coverage["owners"]) == 7 and len({x["owner"] for x in coverage["owners"]}) == 7, "lock owner coverage")
    for name, expected in coverage["logs"].items():
        raw = archive[locks[name]]
        require(len(raw) == expected["bytes"] and digest(raw) == expected["sha256"], "lock coverage log hash")
    for owner in coverage["owners"]:
        require(owner["red"] and owner["green"] and owner["clippy"], "lock owner checks missing")
        require(all(b"FAILED" in archive[locks[name]] for name in owner["red"]), "lock owner behavior Red missing")
        require(all(b"test result: ok." in archive[locks[name]] and b"0 failed" in archive[locks[name]] for name in owner["green"]), "lock owner behavior Green missing")
        require(all(b"error:" not in archive[locks[name]] and b"Finished" in archive[locks[name]] for name in owner["clippy"]), "lock owner Clippy missing")
    cause = value(locks["runtime-observation-lock-root-cause.json"])
    require(cause["diagnostic_exit_code"] != 0, "lock diagnostic did not reproduce")
    parent, child = cause["selected_fd_samples"]
    require(parent["pid"] == cause["parent_pid"] and child["pid"] == cause["inheriting_child_pid"] != parent["pid"], "lock inherited child identity")
    require(parent["path"] == child["path"] == cause["lock_path"] and parent["fd"] == child["fd"] == 31, "inherited lock descriptor")
    for sample in [parent, child]:
        require(f"ino:\t{cause['inode']}\n" in sample["fdinfo"] and "FLOCK" in sample["fdinfo"], "inherited kernel inode or lock sample")
    trace_lines = cause["trace_lines"]
    require([row["line"] for row in trace_lines] == [39, 83, 1079, 1081, 1082, 1094], "lock trace provenance lines")
    times = [float(row["text"].split()[1]) for row in trace_lines]
    require(times == sorted(times) and parent["at"] < child["at"] < times[-1], "inherited lock/exec chronology")
    trace = "".join(row["text"] for row in trace_lines)
    require("close(31<" in trace and "close(104<" in trace and "EAGAIN" in trace and "execve resumed" in trace and "DELAYED" in trace, "missing lock-close/failed-reopen/exec trace")
    require(value(locks["runtime-observation-check-rust-attempt1-meta.json"])["exit_code"] != 0, "initial gate failure omitted")
    for family in ["journal", "app", "blob", "replay"]:
        red_log = archive[locks[f"runtime-observation-{family}-lock-red.log"]]
        green_log = archive[locks[f"runtime-observation-{family}-lock-green.log"]]
        require(b"FAILED" in red_log and b"test result: ok." in green_log and b"0 failed" in green_log, "lock behavior Red/Green missing")
    require(b"4 failed" in archive[locks["runtime-observation-lock-delayed-green.log"]], "intermediate remaining blob-lock failure omitted")
    delayed = archive[locks["runtime-observation-lock-delayed-green3.log"]]
    require(b"test result: ok." in delayed and b"0 failed" in delayed, "concurrent delayed-exec lock recovery missing")
    attempts = document["app_fixture_attempts"]
    for name in ["runtime-observation-app-invalidpath-json", "runtime-observation-app-mirror-premature-runtime-observation-app-acceptance.json"]:
        require(value(attempts["files"][name])["passed"] is False, "failed fixture attempt reported success")
    app = verify_app(document["app_observation"], value, archive, names, source)
    return {"status": "verified", "files": len(archive), "signatures": signatures, "memfd_samples": samples, "source_inputs": source["file_count"], "binaries": 8, "scenarios": len(cases), "host_tests": 6, "app": app}



if __name__ == "__main__":
    try:
        print(json.dumps(verify()))
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        sys.exit(1)
