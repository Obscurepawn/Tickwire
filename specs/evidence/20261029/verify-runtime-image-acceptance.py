#!/usr/bin/env python3
"""Verify the adjacent historical evidence package without reading runtime paths.

Requires Python 3.11+ and cryptography (``python3 -m pip install cryptography``).
Checks archived bytes, signatures and recorded bindings; does not rerun processes,
read /tmp, validate today's source tree, or reconstruct the omitted recovery CAS.
"""

import hashlib
import json
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


def verify():
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ImportError as error:
        raise ValueError("cryptography is required: python3 -m pip install cryptography") from error

    base = Path(__file__).resolve().parent

    def read(relative):
        path = Path(relative)
        require(not path.is_absolute() and ".." not in path.parts, "nonlocal evidence path")
        path = base / path
        require(path.resolve().is_relative_to(base), "evidence path leaves package")
        require(not any(p.is_symlink() for p in [path, *path.parents] if p != base), "symlink evidence")
        require(path.is_file() and path.stat().st_size <= 4 * 1024 * 1024, "evidence file bounds")
        return path.read_bytes()

    document = decode(read("runtime-image-acceptance.json"))
    require(document["schema"] == 1, "unsupported evidence schema")
    archive = {}
    for name, expected in document["artifacts"].items():
        require(name.startswith("runtime-image-acceptance/"), "artifact outside bundle")
        raw = read(name)
        require(len(raw) == expected["bytes"] and digest(raw) == expected["sha256"], f"artifact digest: {name}")
        archive[name] = raw
    listed = set()
    for path in (base / "runtime-image-acceptance").rglob("*"):
        require(not path.is_symlink(), "symlink in bundle")
        if path.is_file():
            listed.add(path.relative_to(base).as_posix())
    require(listed == set(archive), "unlisted or missing bundle file")

    def value(reference):
        return decode(archive[reference])

    source = document["source_binding"]
    require(source["manifest"] == "runtime-image-source-sha256.json", "different source manifest")
    raw = read(source["manifest"])
    require(digest(raw) == source["manifest_sha256"], "historical source manifest digest")
    manifest = decode(raw)
    require(manifest["file_count"] == len(manifest["files"]) == source["file_count"], "source input count")
    require(manifest["git_head"] == source["git_head"], "source base revision")
    for reference in source["verification_logs"]:
        require(archive[reference] == f"UNCHANGED: {source['file_count']} build inputs\n".encode(), "source verification log")

    build = document["build"]
    binaries = value(build["binary_manifest"])
    require(build["exit_code"] == binaries["build_exit_code"] == 0, "build failed")
    require(binaries["source_manifest_sha256"] == source["manifest_sha256"], "binary source binding")
    require(len(binaries["binaries"]) == build["binary_count"] == 8, "binary count")
    names = {row["name"]: row for row in binaries["binaries"]}
    require(len(names) == 8, "duplicate binary")
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
    require(host["worker_sha256"] == metadata["worker_sha256"] == worker["deployed_sha256"], "Host worker identity")
    log = archive[host["files"]["host/process.log"]]
    require(b"test result: ok. 3 passed; 0 failed; 0 ignored;" in log, "Host test counts")
    result = value(host["files"]["host/rebuild-result.json"])
    require(result["pid"] == host["rebuild_pid"] and result["probe_pid"] == host["probe_pid"], "Host rebuild PID")
    require(bytes(result["worker_sha256"]).hex() == worker["deployed_sha256"], "Host rebuild binary")
    require(result["C"]["sequence"] == host["installed_sequence"] and result["D"]["sequence"] == host["continued_sequence"], "Host continued input prefix")
    return {"status": "verified", "files": len(archive), "signatures": signatures, "memfd_samples": samples, "source_inputs": source["file_count"], "binaries": 8, "scenarios": len(cases), "host_tests": 3}


if __name__ == "__main__":
    try:
        print(json.dumps(verify()))
    except Exception as error:
        print(json.dumps({"status": "failed", "error": f"{type(error).__name__}: {error}"}))
        sys.exit(1)
