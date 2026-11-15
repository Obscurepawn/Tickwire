# tickwire-config

`tickwire-config` (`#![forbid(unsafe_code)]`) owns pure, static versioned configuration parsing (`Candidate` from JSON or TOML), schema and
cross-field semantic validation, per-process `Projection` / `ProjectionObject` derivation, full-candidate `PreparedCandidate` normalization, and
deterministic rollout `plan` generation. Runtime configuration patching, local WAL/journal stores, and disk materializers are intentionally absent:
all production processes load static startup configuration only and hold zero filesystem or journal persistence authority here.

## Invariants and Change Guidelines

* **Strict static parsing and canonical hashing**: `Candidate::parse` ([src/lib.rs](src/lib.rs)) and `strict` reject oversized payloads (`<= 16 MiB`),
  excessive nesting (`max_depth <= 64`), duplicate object keys, non-finite floats, and ambiguous scalars before emitting deterministic
  `canonical` JSON bytes and SHA-256 `Hash` digests that remain invariant across TOML/JSON round-trips.
* **Schema and cross-field semantics**: Keep structural JSON Schema validation ([src/validation.rs](src/validation.rs)) synchronized with
  [system-config.schema.json](../../specs/contracts/system-config.schema.json) and
  [deployment-reference-v1.json](../../specs/contracts/deployment-reference-v1.json), enforcing capacity budgets
  ([src/capacity.rs](src/capacity.rs)), artifact digests ([src/artifacts.rs](src/artifacts.rs)), and locator rules
  ([src/locator.rs](src/locator.rs)).
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
