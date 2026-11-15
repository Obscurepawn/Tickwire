# tickwire-config

`tickwire-config` (`#![forbid(unsafe_code)]`) owns pure, static versioned configuration parsing (`Candidate` from JSON or TOML), schema and
cross-field CPU/ORS semantic validation, per-process `Projection` / `ProjectionObject` derivation, full-candidate `PreparedCandidate` normalization,
and deterministic rollout `plan` generation. Runtime configuration patches, local write-ahead logs, and disk blob files are absent: all production
processes load immutable static startup configuration only and hold zero filesystem persistence authority here.

## Invariants and Change Guidelines

* **Strict static parsing and canonical hashing**: `Candidate::parse` ([src/lib.rs](src/lib.rs)) and `strict` reject oversized payloads (`<= 16 MiB`),
  excessive nesting (`max_depth <= 64`), duplicate object keys, non-finite or floating-point numbers, and ambiguous scalars before emitting
  deterministic `canonical` JSON bytes and SHA-256 `Hash` digests that remain invariant across TOML/JSON round-trips.
* **Schema, CPU isolation, and multi-UID ORS semantics**: Keep structural JSON Schema validation and `validation::check` ([src/validation.rs](src/validation.rs))
  synchronized with [system-config.schema.json](../../specs/contracts/system-config.schema.json) and
  [deployment-reference-v1.json](../../specs/contracts/deployment-reference-v1.json):
  * CPU IDs strictly `< 4096`; `req_cpu != resp_cpu` when both are set; strictly ascending, deduplicated `housekeeping_cpus`; and disjoint hot vs.
    housekeeping CPUs (`hot_cpus ∩ housekeeping_cpus == ∅`) across host and process definitions plus physical-core `CapacityAssessment`
    ([src/capacity.rs](src/capacity.rs)).
  * Stateless multi-UID ORS bounds on `exec` processes: `epoll_wait_ms` (default `0`), `max_order_age_ns` (default `23000`), power-of-two
    `order_manager.slots_per_strat`, unique `enabled_accounts` `account_ref` in `1..=255` with `[local_ip, count]` URL pools, and monotonic `rms`
    decimal limits (`max_order_notional <= max_pos_notional`, `fat_finger_ratio` in `(0, 1]`, and `1s <= 5s <= 10s` rolling windows).
* **Scoped projections and encoded rollout plans**: `projection` ([src/projection.rs](src/projection.rs)), `projection_object`
  ([src/projection_object.rs](src/projection_object.rs)), and `materialized` ([src/materialized.rs](src/materialized.rs)) isolate each target
  process's visible slice and render stamp, while [src/prepared.rs](src/prepared.rs) (`Head`, `Input::Full`, `PreparedCandidate`) and
  [src/plan.rs](src/plan.rs) (`dependencies`, `rules`, `encoded`) order multi-target rollout dependencies and parameter deliveries
  ([src/delivery.rs](src/delivery.rs)).

## Verification and Specifications

* **Source**: [src/lib.rs](src/lib.rs) · [src/validation.rs](src/validation.rs) · [src/projection.rs](src/projection.rs) ·
  [src/projection_object.rs](src/projection_object.rs) · [src/prepared.rs](src/prepared.rs) · [src/plan.rs](src/plan.rs) ·
  [src/materialized.rs](src/materialized.rs) · [src/locator.rs](src/locator.rs)
* **Integration tests**: [tests/candidate.rs](tests/candidate.rs) · [tests/static_semantics.rs](tests/static_semantics.rs) ·
  [tests/projection.rs](tests/projection.rs) · [tests/plan.rs](tests/plan.rs) · [tests/plan_archive.rs](tests/plan_archive.rs) ·
  [tests/prepared.rs](tests/prepared.rs) · [tests/delivery.rs](tests/delivery.rs) · [tests/capacity.rs](tests/capacity.rs) ·
  [tests/artifacts.rs](tests/artifacts.rs) · [tests/changes.rs](tests/changes.rs) · [tests/locator.rs](tests/locator.rs) ·
  [tests/parameter_values.rs](tests/parameter_values.rs) · [tests/independent_review.rs](tests/independent_review.rs)
* **Specifications and contracts**: [config-candidate.md](../../specs/implementation/config-candidate.md) ·
  [config-plan-archive.md](../../specs/implementation/config-plan-archive.md) · [config-locator.md](../../specs/implementation/config-locator.md)
  · [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md) ·
  [system-config.schema.json](../../specs/contracts/system-config.schema.json)

```sh
cargo test --locked -p tickwire-config --all-targets
python3 scripts/verify-config-contract.py
```
