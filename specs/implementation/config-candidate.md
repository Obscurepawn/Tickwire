# Configuration Candidates, Scoped Projections, Capacity Assessment, and Rollout Plans

`tickwire-config` parses raw JSON or TOML configuration documents into canonical, content-addressed `Candidate` values and executes pure, deterministic static validation, artifact catalog verification, permission-scoped `ProjectionDelivery`, host `CapacityAssessment`, and `ComputedPlan` rollout synthesis.

The crate is intentionally side-effect-free: it reads neither wall clocks nor live process leases, performs no network or filesystem retrieval, and confers no execution authority. Control-plane sessions, `tickwire-config-materializer`, and individual market, strategy, and account processes independently authenticate rollout commits, materialize local launch files, and emit runtime application receipts.

## State Model and Bounds

`Candidate::parse` rejects duplicate JSON object keys (including `\uXXXX`-escaped equivalents) during streaming deserialization, forbids floating-point numbers, TOML datetimes, float-syntax zeros (`-0e0`), and integers outside IEEE-754 exact integer range (`±(2^53 - 1)`), normalizes integer `-0` to `0`, preserves array order, and sorts object keys in RFC 8785 UTF-16 code-unit order before computing `SHA-256`.

| Artifact or Stage | Enforced Capacity / Structural Bound |
| --- | --- |
| `Candidate` input & AST | `<= 16 MiB` (`max_bytes`), `<= 64` nesting levels (`32` default; `16` for strategy parameters), `<= 262,144` value nodes (`MAX_NODES`), integers in `±9,007,199,254,740,991` |
| Artifact catalog & parameter schemas | `<= 4,096` artifacts (`<= 16 MiB` each, `<= 512 MiB` total); parameter schemas `<= 1 MiB` and `32` levels (local `$ref` only; linear regex engine with `1 MiB` compiled + `1 MiB` DFA caches) |
| `ProjectionDelivery` & `ProjectionObject` | Revisions in `1..=2^63 - 1`; `<= 4,096` process targets and `<= 512 MiB` aggregate delivery payloads; each `ProjectionObject` `<= 4 MiB` (and configured delivery limit) |
| Materialized `ProjectionFile` & `ComputedPlan` | Process-specific rendered TOML `ProjectionFile` `<= 8 MiB`; canonical JSON `ComputedPlan` `<= 1 MiB` hashed with prefix `"tickwire.config.computed-plan.v1\0"` |

## Core Invariants

* **Offline schema validation and explicit normalization**: `validate_structural()` evaluates the embedded Draft 2020-12 `system-config.schema.json` with external URI resolution disabled. `normalized_configuration()` injects the sole schema default (`RoleBinding.cluster_scope = false`), re-verifies byte and node budgets, and computes the normalized `SHA-256` (whereas generic `Candidate::parse` preserves the exact caller structure). `validate_static()` verifies identifier uniqueness, cross-references, exact decimal risk budgets, checked byte arithmetic, single local writer per account, single publisher per venue-bound topic, shared-memory ring/arena geometry, and deadline ordering.
* **Host-inventory `CapacityAssessment` and core isolation**: OS CPU and NUMA IDs need not be contiguous; `CapacityAssessment` binds an explicit host inventory and retained-resource snapshot, verifying failure domains, CPU-to-NUMA/socket/physical-core topology, reserved private and shared memory, and SHM ceilings while forbidding any hot-loop worker and housekeeping task (including the host supervisor) from sharing a physical core.
* **Least-privilege `PinnedPolicy` filtering and complete `ProjectionDelivery`**: `PinnedPolicy::decode` verifies the whole-policy digest and restricts each target process to approved accounts, venues, topics, and explicit credential purposes within its role ceiling; strategies receive zero credential purposes, and unauthorized `secret://` references embedded inside parameter values fail validation. `ProjectionDelivery` covers the full union of old and new process IDs (`Added`, `Retired`, `Changed`, `RevisionOnly`) so callers cannot omit unchanged processes from a global revision advance, while `Retired` targets retain their prior permission-filtered payload alongside the new `global_hash`. `ProjectionFile` stamps `applied_event_seq = 0` for `Desired` files and requires non-zero boot and `applied_event_seq` values (plus non-zero UUID `operation_id` and `instance_slot_id`) for `Effective` files.
* **Asymmetric risk-relaxation and scope-bound dual authority**: `ChangeSet::between` aligns array entities by stable ID and classifies relative reorderings explicitly. Tightening an existing risk budget may classify `Hot`, whereas relaxing any protection—including raising disk-warning/freeze thresholds or widening receive windows—adding budgets, adding or retiring strategies, overriding instrument contracts, or altering tradable symbols requires a `Quiesce` boundary and *both* operator and risk-admin permissions within the *same* account scope (an operator on account A and risk-admin on account B cannot combine; accountless policies require cluster-admin authority; all strategy-owned parameter fields classify `Quiesce` regardless of field name).

## Operations and Failure Semantics

* **Dependency-propagated `ComputedPlan` synthesis**: `ComputedPlan::compute(base_revision, base, prepared, delivery, options)` assigns every target the highest lifecycle action (`NoOpRevisionAdvance`, `Hot`, `Quiesce`, `RollingRestart`, or `Retire`) demanded across its old and new dependency graph: account, risk, and topic changes propagate to responsible execution, protection, publication, and subscription processes without spilling account-only risk edits onto unrelated market publishers; process-level edits propagate to the host supervisor and dependent accounts/topics; and generic IPC geometry, control-plane, or qualification changes propagate cluster-wide. Even byte-identical process payloads inherit required actions when shared dependencies change. Default options apply a `RollingRestart` ceiling, `RejectIfExceeded` exposure policy, and empty target hints; unknown target hints, attempts to shrink the target set, actions above `apply_ceiling`, `ForbiddenRuntime` edits, or unclassifiable projected changes fail plan construction.
* **Recomputed plan verification and compile-time `rules_digest()`**: `ComputedPlan::verify_encoded` recomputes the complete plan from `(base_revision, base, prepared, delivery)` and demands byte-for-byte equality with an archived `EncodedPlan`, ensuring a valid digest over tampered target rows cannot pass. Separately, `plan::rules_digest()` (`"tickwire.config.planning-rules.v1\0"`) hashes a length-framed compile-time manifest of the 19 planner, classifier, codec, schema, `Cargo.toml`, `Cargo.lock`, and `rust-toolchain.toml` files so archives can detect planner-version drift outside the plan encoding.

## Verification and References

```sh
cargo test --locked -p tickwire-config
cargo clippy --locked -p tickwire-config --all-targets -- -D warnings
```

Test suites exercise strict JSON/TOML scalar and duplicate-key rejection, RFC 8785 canonicalization, linear-time parameter-schema regex bounds, `PinnedPolicy` secret smuggling rejection, NUMA/physical-core `CapacityAssessment`, dual-authority risk relaxation rules, dependency propagation, and `ComputedPlan` round-trip verification.

* Core parser, validator, artifacts, projections, capacity, and planner: [lib.rs](../../crates/tickwire-config/src/lib.rs), [strict.rs](../../crates/tickwire-config/src/strict.rs), [validation.rs](../../crates/tickwire-config/src/validation.rs), [artifacts.rs](../../crates/tickwire-config/src/artifacts.rs), [projection_object.rs](../../crates/tickwire-config/src/projection_object.rs), [capacity.rs](../../crates/tickwire-config/src/capacity.rs), [changes.rs](../../crates/tickwire-config/src/changes.rs), [plan.rs](../../crates/tickwire-config/src/plan.rs)
* Machine schemas: [system-config.schema.json](../contracts/system-config.schema.json), [config-patch.schema.json](../contracts/config-patch.schema.json)
* Test suites: [candidate.rs](../../crates/tickwire-config/tests/candidate.rs), [plan.rs](../../crates/tickwire-config/tests/plan.rs)
* Related specifications: [Archived rollout plans](config-plan-archive.md), [Configuration locator](config-locator.md), [Local configuration materializer](config-materializer.md)
