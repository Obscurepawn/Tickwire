#!/usr/bin/env python3
"""Read-only verification of an archived strategy-parameter acceptance package.

Python 3.11+ standard library only. The package contains historical process logs
and small immutable files, not executables or the complete recovery closure.
This checks those bytes and their recorded bindings; it does not rerun workers,
prove a past kernel exit independently, submit orders, or establish global Applied.

Usage: python3 verify-strategy-parameters.py [strategy-parameters.json]
"""
import hashlib
import json
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


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def unsupported(value):
        raise ValueError(f"non-integer JSON number: {value}")

    return json.loads(raw, object_pairs_hook=unique,
                      parse_float=unsupported, parse_constant=unsupported)


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


def command_value(raw):
    """Read this fixture's deterministic venue CBOR; no economic revalidation."""
    C.require(0 < len(raw) <= 16_384, "command byte bounds")
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
        C.require(depth <= 32 and nodes <= 16_384, "command CBOR bounds")
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


def verify(manifest_path):
    base = manifest_path.resolve().parent

    def read(relative, limit=32 * 1024 * 1024):
        C.require(isinstance(relative, str), "evidence path type")
        path = Path(relative)
        C.require(not path.is_absolute() and ".." not in path.parts, "nonlocal evidence path")
        path = base / path
        C.require(path.resolve().is_relative_to(base), "evidence path escapes package")
        C.require(not any(p.is_symlink() for p in [path, *path.parents] if p != base),
                  "symlink evidence path")
        C.require(path.is_file() and 0 <= path.stat().st_size <= limit, "evidence file bounds")
        return path.read_bytes()

    document = decode(read(manifest_path.name, 4 * 1024 * 1024))
    C.require(document["schema"] == 1, "unsupported evidence schema")
    archive = {}
    total = 0
    for path, expected in document["artifacts"].items():
        raw = read(path)
        C.require(len(raw) == expected["bytes"] and sha(raw) == expected["sha256"],
                  f"archived bytes changed: {path}")
        total += len(raw)
        C.require(total <= 128 * 1024 * 1024, "archive total exceeds 128 MiB")
        archive[path] = raw

    def raw_file(path):
        C.require(path in archive, f"unlisted referenced artifact: {path}")
        return archive[path]

    def value(path):
        return decode(raw_file(path))

    source = document["source_binding"]
    source_raw = read(source["manifest"], 4 * 1024 * 1024)
    C.require(sha(source_raw) == source["manifest_sha256"], "source manifest binding")
    binaries = value(document["build"]["binary_manifest"])
    C.require(binaries["build_exit_code"] == 0, "binary build failed")
    C.require(binaries["source_manifest_sha256"] == source["manifest_sha256"],
              "binary/source manifest mismatch")
    workers = [row for row in binaries["binaries"] if row["name"] == "tickwire-counting-worker"]
    C.require(len(workers) == 1, "counting worker manifest ambiguity")
    worker_hash = workers[0]["deployed_sha256"]
    C.require(re.fullmatch(r"[0-9a-f]{64}", worker_hash) is not None, "worker hash format")

    app = document["app_process"]
    C.require(app["exit_code"] == 0, "App process tests failed")
    log = raw_file(app["log"]).decode("utf-8")
    C.require("test result: ok. 6 passed; 0 failed; 0 ignored;" in log, "actual six-test summary")
    names = {
        "projected_parameters_reject_mismatch_before_worker_start",
        "registered_parameters_fixture_runs_without_projection_binding",
        "projected_parameters_reach_real_registered_consumer",
        "projected_observation_and_bid_thresholds_change_actual_worker_effects",
        "pending_parameter_query_holds_an_actual_queued_intent_until_fresh_proof",
        "expired_parameter_query_rejects_queued_intent_and_reaps_worker",
    }
    for name in names:
        C.require(f"test {name} ... ok" in log, f"missing passed test: {name}")
    events = [decode(line) for line in log.splitlines() if line.startswith("{")]
    consumed = [row for row in events if row.get("case") == "parameter_consumed"]
    thresholds = [row for row in events if row.get("case") == "parameter_threshold"]
    pending = [row for row in events if row.get("case") == "pending_parameter_gate"]
    expired = [row for row in events if row.get("case") == "parameter_query_expired"]
    C.require((len(consumed), len(thresholds), len(pending), len(expired)) == (5, 3, 1, 1),
              "unexpected parameter event counts")
    fixtures = {f["original_root"]: f for f in app["fixtures"]}
    C.require(len(fixtures) == len(app["fixtures"]) == 5, "fixture mapping cardinality")
    C.require(set(fixtures) == {row["root"] for row in consumed}, "fixture/log root binding")
    challenges = set()
    by_pid = {}
    by_command = {}
    for row in consumed:
        f = fixtures[row["root"]]
        challenge = octets(row["challenge"], 16)
        C.require(any(challenge) and challenge not in challenges, "zero or repeated challenge")
        challenges.add(challenge)
        C.require(type(row["pid"]) is int and row["pid"] > 0 and row["pid"] not in by_pid,
                  "invalid or duplicate recorded worker PID")
        C.require(row["global_revision"] == 41 and row["private_revision"] == 6,
                  "global/private revision identity")
        C.require(hash_array(row["worker_binary"]) == worker_hash, "observed worker binary")
        private_raw = raw_file(f["parameters"])
        schema_raw = raw_file(f["schema"])
        private = decode(private_raw)
        object_matches(private_raw, row["private_object"], "private configuration")
        C.require(sha(schema_raw) == hash_array(row["schema_hash"]), "raw schema hash")
        C.require(sha(candidate_bytes(private)) == hash_array(row["parameter_hash"]),
                  "canonical parameter value hash")
        projection_raw = raw_file(f["projection"])
        projection = decode(projection_raw)
        C.require(projection_raw == candidate_bytes(projection), "canonical projection encoding")
        C.require(projection["global_revision"] == "41", "projection global revision")
        payload = projection["payload"]
        C.require(projection["projection_hash"] == "sha256:" + sha(candidate_bytes(payload)),
                  "projection payload hash")
        C.require(payload["strategy"]["params"] == private, "projection/private parameter values")
        C.require(artifact_hash(payload["strategy"]["parameter_schema_ref"]) == sha(schema_raw),
                  "projection schema reference")
        C.require(artifact_hash(payload["process"]["artifact_ref"]) == worker_hash,
                  "projection binary reference")
        bootstrap = value(f["bootstrap"])
        binding = bootstrap["factory"]
        C.require(binding["id"] == "counting_example" and binding["revision"] == 6,
                  "registered factory binding")
        C.require(binding["configuration"] == row["private_object"], "Bootstrap private object")
        checkpoint_raw = raw_file(f["checkpoint"])
        object_matches(checkpoint_raw, row["checkpoint"], "observed current checkpoint")
        cp = decode(checkpoint_raw)
        C.require(serde_bytes(cp) == checkpoint_raw and cp["schema"] == 2, "checkpoint canonical codec")
        C.require(cp["cut"] == row["current_cut"], "current cut differs from logged observation")
        C.require(cp["runtime"]["config"]["revision"] == 6 and
                  octets(cp["runtime"]["config"]["bytes"]) == private_raw, "current private config bytes")
        object_matches(octets(cp["business"]), cp["cut"]["business"], "business state")
        object_matches(serde_bytes(cp["runtime"]), cp["cut"]["runtime"], "runtime state")
        identity = bootstrap["market"]["local"]
        for key in ("instance", "generation", "logical_strategy"):
            C.require(cp["source"][key] == identity[key], f"current source {key}")
        C.require(hash_array(cp["source"]["binary_digest"]) == worker_hash ==
                  hash_array(identity["build_hash"]), "current source/Bootstrap binary")
        C.require(cp["source"]["clock"] == {"boot_id": identity["host_boot"],
                  "generation": bootstrap["clock_generation"]}, "logical clock binding")
        C.require(("command" in f) == ("descriptor" in f), "partial command evidence")
        command_hash = None
        if "command" in f:
            command_raw = raw_file(f["command"])
            descriptor = value(f["descriptor"])
            command_hash = sha(command_raw)
            C.require(command_hash == hash_array(descriptor["command_digest"]),
                      "descriptor/actual command digest")
            recovery = descriptor["recovery"]
            C.require(recovery["owner"] == identity["instance"], "command descriptor owner")
            C.require(Path(recovery["inputs"]).parent == Path(row["root"]) / "recovery",
                      "command descriptor fixture root")
            images = [dep for dep in recovery["dependencies"] if dep["kind"] == 8]
            C.require(len(images) == 1 and hash_array(images[0]["digest"]) == worker_hash and
                      images[0]["owner"] == identity["instance"], "command descriptor worker")
            bind_command(command_raw, octets(private["command"]), identity)
            C.require(command_hash not in by_command, "ambiguous command-to-fixture binding")
            by_command[command_hash] = (row, private)
        by_pid[row["pid"]] = (row, private, command_hash)

    combinations = set()
    for row in thresholds:
        command_hash = hash_array(row["intent_digest"])
        C.require(command_hash in by_command, "threshold command has no consumed fixture")
        consumed_row, private = by_command[command_hash]
        combination = (row["minimum_observations"], row["minimum_bid_ticks"])
        C.require(combination == (private["minimum_observations"], private["minimum_bid_ticks"]),
                  "threshold parameter binding")
        combinations.add(combination)
        C.require(row["effects"] == 1 and 0 < row["initial_observations"] < 8,
                  "recorded initial callbacks or one-shot effect")
        C.require(row["final_observations"] >= max(row["initial_observations"], row["minimum_observations"]),
                  "recorded final callback count")
        C.require(row["no_gateway_submission"] is True, "threshold scope changed")
    C.require(combinations == {(1, 100), (8, 100), (1, 101)}, "threshold scenario set")
    row = pending[0]
    C.require(row["pid"] in by_pid and hash_array(row["intent_digest"]) == by_pid[row["pid"]][2],
              "pending released a different command/process")
    C.require(row["effect_before_query"] == 1 and row["pending_delivered"] is False and
              row["fresh_proof_then_same_intent"] is True and row["no_gateway_submission"] is True,
              "pending gate outcome")
    row = expired[0]
    C.require(type(row["pid"]) is int and row["pid"] > 0 and row["pid"] not in by_pid,
              "expired worker reported successful consumption")
    C.require(row["effect_was_pending"] is True and row["delivered"] is False and
              row["proof"] is False and row["reaped"] is True, "expired query outcome")
    C.require(document["scope"]["gateway_submission"] is False and
              document["scope"]["global_applied"] is False, "acceptance scope overclaim")
    return {"result": "PASS", "evidence_mode": "development" if document.get("development_only") else "archived", "checks": C.count, "archived_files": len(archive),
            "consumed_workers": 5, "independent_challenges": 5, "threshold_cases": 3,
            "pending_cases": 1, "expired_cases": 1, "gateway_submission": False,
            "global_applied": False,
            "boundary": "Historical bytes and logged assertions only; no complete recovery replay or independent past-exit proof."}


if __name__ == "__main__":
    try:
        path = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).with_name("strategy-parameters.json")
        if len(sys.argv) > 2:
            raise ValueError("usage: verify-strategy-parameters.py [manifest.json]")
        print(json.dumps(verify(path), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, IndexError, RecursionError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
