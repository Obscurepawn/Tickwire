# Archived Rollout Plans, Strict Re-Decoding, and Rules Provenance

`tickwire-config::plan::EncodedPlan` provides the canonical serialized representation for persisting, reviewing, and re-opening a `ComputedPlan` across control-plane approval and rollout stages. Saving a plan captures the exact base configuration identity, successor revision, planner options, affected accounts, per-process projection hashes, required transition classes, and execution preconditions in a single domain-separated `SHA-256` artifact.

Decoding an `EncodedPlan` proves only that the saved JSON document is canonically formatted, structurally self-consistent, and matches its expected digest. Verifying that the plan faithfully reflects a specific static configuration transition requires `ComputedPlan::verify_encoded`, and proving that running processes launched with a given static projection requires independent supervisor and runtime observation checks.

## State Model and Bounds

An `EncodedPlan` is an RFC 8785 canonical JSON object bounded by `MAX_BYTES = 1 MiB` (`1,048,576` bytes) and `max_depth = 16`. Its content digest is computed over `"tickwire.config.computed-plan.v1\0" || canonical_bytes`, isolating plan digests from raw `Candidate` and `ProjectionObject` hashes.

| Layer or Field | Required Structure / Bound |
| --- | --- |
| Envelope (`schema = 1`) | Exact 14-field root object (`classification_version = 1`, `base_revision`, `next_revision = base_revision + 1`, `base_hash`, `base_etag`, `candidate_hash`, `required_class`, `requires_risk_admin`, `requires_cluster_admin`, `affected_accounts`, `options`, `targets`, `preconditions`) |
| Revision & ETag binding | Canonical decimal strings in `1..=2^63 - 1` (no leading zeros or signs); `base_etag` must equal `"\"cfg-<base_revision>-<base_hash_hex>\""` |
| Root `preconditions` | Strictly sorted, duplicate-free `[CurrentAuthority, ContentQualification, RetainedRegistryHead]` |
| `targets` map (`1..=4,096`) | Keyed by valid process IDs; each entry binds `action`, `required_class`, `old_projection`, `new_projection`, `old_host`, `new_host`, `accounts`, `changes` (JSON Pointers), and target `preconditions` |
| `plan::rules_digest()` | Domain-separated `SHA-256` (`"tickwire.config.planning-rules.v1\0"`) over 17 length-framed source, schema, Cargo manifest/lockfile, and toolchain inputs; stored alongside—never inside—the plan bytes |

## Core Invariants

* **Strict canonicality and ordered-set enforcement**: `EncodedPlan::decode(bytes, expected_digest)` parses with `Candidate::parse`, requires `parsed.canonical_bytes() == bytes` (rejecting extra whitespace, duplicate keys, non-RFC-8785 key ordering, floats, and unknown fields at every nested layer), verifies the domain-separated `SHA-256` digest, and requires strictly ascending, duplicate-free arrays for `requested_target_ids`, `affected_accounts`, root `preconditions`, per-target `accounts`, and per-target `preconditions`.
* **Action-to-projection-and-precondition consistency**: Every target entry must include `RuntimeIdentity` and `RuntimeCapability`, include `EffectiveBaseline` if and only if `old_projection` is non-null, bind `old_projection.global_hash == base_hash` and `new_projection.global_hash == candidate_hash`, and match its declared `action`:
  * `NoOpRevisionAdvance`: `required_class == null`, `old == new` (both present), and empty `changes`.
  * `Hot`: `required_class == Hot`, both projections present, and `apply_ceiling` permits `Hot`.
  * `Quiesce`: `required_class == Quiesce`, both projections present, and `Quiescence` precondition present.
  * `RollingRestart`: `required_class == RollingRestart`, `new_projection` present, `PlannedSuccessor` present, and `Quiescence` present whenever `old_projection` exists.
  * `Retire`: `required_class == RollingRestart`, `old_projection` present, `new_projection == null`, and both `Retirement` and `Quiescence` present.

## Operations and Failure Semantics

* **Full recomputation against forged or stale plans (`verify_encoded`)**: Because a freshly computed digest over hand-edited JSON cannot prove that planner dependency rules were honored, callers re-validating an archived plan pass `(encoded, base_revision, base, prepared, delivery)` to `ComputedPlan::verify_encoded`. The planner recomputes the transition from scratch using the current compiled rules (identifiable via `plan::rules_digest()`) and rejects the archive unless the freshly generated canonical bytes match `encoded.canonical_bytes()` byte for byte.

## Verification and References

```sh
cargo test --locked -p tickwire-config --test plan_archive
cargo clippy --locked -p tickwire-config --all-targets -- -D warnings
```

The `plan_archive` suite tests full plan round-tripping, `verify_encoded` base-revision mismatch rejection, `rules_digest()` stability, and exhaustive negative mutations across missing/unknown fields, enum tags, reversed or duplicated ordered sets, malformed decimal strings, uppercase or truncated hashes, and broken action/precondition pairings.

* Encoded plan verifier and rules digest: [encoded.rs](../../crates/tickwire-config/src/plan/encoded.rs), [rules.rs](../../crates/tickwire-config/src/plan/rules.rs), [plan.rs](../../crates/tickwire-config/src/plan.rs)
* Archive round-trip and mutation tests: [plan_archive.rs](../../crates/tickwire-config/tests/plan_archive.rs)
* Related specifications: [Configuration candidates and rollout plans](config-candidate.md), [Configuration locator](config-locator.md)
