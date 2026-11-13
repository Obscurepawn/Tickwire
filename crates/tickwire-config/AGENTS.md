# tickwire-config

`tickwire-config` (`#![forbid(unsafe_code)]`) owns pure, versioned configuration parsing (`Candidate` from JSON or TOML), schema and cross-field
semantic validation, per-process `Projection` / `ProjectionObject` derivation, patch application, and rollout `plan` generation. It holds zero
filesystem, journal, or process-supervision authority; persistence and activation belong in `tickwire-config-fs`, `tickwire-config-journal`, and
`tickwire-config-materializer`.

## Invariants and Change Guidelines

* **Strict parsing and canonical hashing**: `Candidate::parse` ([src/lib.rs](src/lib.rs)) and `strict` reject oversized payloads (`<= 16 MiB`),
  excessive nesting (`max_depth <= 64`), duplicate object keys, non-finite floats, and ambiguous scalars before emitting deterministic
  `canonical` JSON bytes and SHA-256 `Hash` digests that remain invariant across TOML/JSON round-trips.
* **Schema and cross-field semantics**: Keep structural JSON Schema validation ([src/validation.rs](src/validation.rs)) synchronized with
  [system-config.schema.json](../../specs/contracts/system-config.schema.json),
  [config-patch.schema.json](../../specs/contracts/config-patch.schema.json), and
  [deployment-reference-v1.json](../../specs/contracts/deployment-reference-v1.json), enforcing capacity budgets
  ([src/capacity.rs](src/capacity.rs)), artifact digests ([src/artifacts.rs](src/artifacts.rs)), and locator rules
  ([src/locator.rs](src/locator.rs)).
* **Scoped projections and encoded rollout plans**: `projection` ([src/projection.rs](src/projection.rs)), `projection_object`
  ([src/projection_object.rs](src/projection_object.rs)), and `materialized` ([src/materialized.rs](src/materialized.rs)) isolate each target
  process's visible slice and render stamp, while [src/plan.rs](src/plan.rs) (`dependencies`, `rules`, `encoded`) orders multi-target rollout
  dependencies and parameter deliveries ([src/delivery.rs](src/delivery.rs)).

## Verification and Specifications

* **Source**: [src/lib.rs](src/lib.rs) · [src/validation.rs](src/validation.rs) · [src/projection.rs](src/projection.rs) ·
  [src/projection_object.rs](src/projection_object.rs) · [src/patch.rs](src/patch.rs) · [src/plan.rs](src/plan.rs) ·
  [src/materialized.rs](src/materialized.rs) · [src/locator.rs](src/locator.rs)
* **Integration tests**: [tests/candidate.rs](tests/candidate.rs) · [tests/static_semantics.rs](tests/static_semantics.rs) ·
  [tests/projection.rs](tests/projection.rs) · [tests/patch.rs](tests/patch.rs) · [tests/plan.rs](tests/plan.rs) ·
  [tests/plan_archive.rs](tests/plan_archive.rs) · [tests/prepared.rs](tests/prepared.rs) · [tests/delivery.rs](tests/delivery.rs) ·
  [tests/capacity.rs](tests/capacity.rs) · [tests/artifacts.rs](tests/artifacts.rs) · [tests/changes.rs](tests/changes.rs) ·
  [tests/locator.rs](tests/locator.rs) · [tests/parameter_values.rs](tests/parameter_values.rs) ·
  [tests/independent_review.rs](tests/independent_review.rs)
* **Specifications and contracts**: [config-candidate.md](../../specs/implementation/config-candidate.md) ·
  [config-plan-archive.md](../../specs/implementation/config-plan-archive.md) · [config-locator.md](../../specs/implementation/config-locator.md)
  · [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md) ·
  [system-config.schema.json](../../specs/contracts/system-config.schema.json) ·
  [config-patch.schema.json](../../specs/contracts/config-patch.schema.json)

```sh
cargo test --locked -p tickwire-config --all-targets
python3 scripts/verify-config-contract.py
```
