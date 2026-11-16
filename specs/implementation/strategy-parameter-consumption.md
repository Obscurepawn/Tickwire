# Stateless Strategy Parameter Validation and Factory Binding

`tickwire-strategy-sdk::registered` and `tickwire-strategy-sdk::bootstrap::FactoryBinding` bind an immutable startup parameter document to a statically registered strategy executable before the worker joins any shared-memory market topic or opens a `DOP` execution session. Rather than mutating parameters dynamically on the hot path or reading prior run files, each strategy worker validates its static bootstrap configuration, verifies the cryptographic SHA-256 digest and byte length of its private parameter file, constructs the typed strategy instance through `Registration::configure` and `UserFactory::create`, and rejects any schema, fixed-point, or identity mismatch at cold startup.

Cluster-level configuration candidate validation (`config-candidate.md`) and economic instrument catalog verification (`instrument-catalog.md`) complement this worker-side startup contract.

## State Model and Bounds

A registered worker executable (`tickwire-strategy-example` or user binary calling `registered::run`) loads a strict `#[serde(deny_unknown_fields)]` `Bootstrap` JSON file (`<= 512 KiB`) containing an optional `FactoryBinding`:

| Artifact or Parameter | Validation Rule / Enforced Bound |
| --- | --- |
| Trusted file open (`worker::read_trusted_bytes`) | Rejects symlinks (`symlink_metadata().file_type().is_file()`), verifies `(dev, ino)` continuity after `File::open`, rejects group/world-writable permissions (`mode & 0o022 == 0`), and enforces exact byte upper bounds before reading. |
| `Bootstrap` document (`bootstrap.rs`) | Strict `#[serde(deny_unknown_fields)]` JSON (`<= 512 KiB`); validates non-zero `logical_strategy_id`, `strat_id <= 254`, `instance_id != [0; 16]`, `generation >= 1`, `account_ref` (`0..=255`), and optional `worker_cpu` / `housekeeping_cpus` disjointness (`validate_cpu_isolation`). |
| `FactoryBinding` (`bootstrap.rs`, `registered.rs`) | Requires matching `id` (`1..=64` ASCII `[0-9a-zA-Z_-]` bytes, serde alias `"registration_id"`), `state_schema != 0`, `0 < revision < 2^63`, absolute `configuration_file` (`<= 2 MiB`), and exact `(bytes.len(), sha256(bytes)) == (configuration.size_bytes, configuration.sha256)`. |
| Parameter payload (`Parameters::decode`) | Strategy factories enforce `#[serde(deny_unknown_fields)]`, canonical byte round-trip or strict schema rules, integer ticks/lots or checked fixed-point bounds (`0` floating-point fields), and matching `(logical_strategy_id, instance_id, generation, revision)` command templates. |
| Optional `Catalog` (`catalog.rs`) | When `catalog_file` or `instrument_catalog` is supplied (`<= 2 MiB`), `catalog::from_json_bytes` validates exact tick/step economics and confirms every subscribed market instrument matches `(instrument_id, instrument_version, catalog_hash)`. |

## Core Invariants

* **Fail-fast cold startup before network or SHM side effects**: `registered::run` validates CPU isolation, `Bootstrap` bounds, `Catalog` digests, `FactoryBinding` SHA-256/length, `Registration::configure`, and `UserFactory::create` *before* connecting to `tickwire-md` or sending `DopLogin` to the ORS execution process. Any tampered parameter file, trailing whitespace/unknown field, wrong `registration_id`, or mismatched `state_schema` aborts startup immediately with zero orders sent.
* **Immutable parameters for the lifetime of a worker session**: Once `UserFactory::create` returns the running `Strategy`, its parameters remain immutable in private memory throughout the hot loop (`0` runtime parameter-patch polling, `0` lock acquisitions, `0` disk reads on the hot path). Changing strategy parameters uses a clean worker restart (`DopLogout` -> cold boot with the new `FactoryBinding` revision -> `DopLogin` adopting `DopEcho.next_source_order_seqnum`).
* **Audited `#![forbid(unsafe_code)]` user strategy boundary**: Registered strategy crates (`tickwire-strategy-example`) implement `Registration`, `UserFactory`, and `Strategy` in 100% safe Rust (`#![forbid(unsafe_code)]`), delegating shared-memory mapping and thread pinning exclusively to `tickwire-strategy-sdk` and `tickwire-control-sys`.

## Verification and References

```sh
cargo test --locked -p tickwire-strategy-sdk -p tickwire-strategy-example --all-targets
cargo clippy --locked -p tickwire-strategy-sdk -p tickwire-strategy-example --all-targets -- -D warnings
```

Unit and end-to-end tests in `tickwire-strategy-example` and `tickwire-strategy-sdk` verify rejection of unknown JSON fields, trailing bytes, non-positive observation thresholds, negative tick thresholds, tampered SHA-256 digests, mismatched `V1`/`V2` `Registration` schemas, and full end-to-end execution of `Counting` from static `FactoryBinding` configuration through `JOIN`, `DopLogin`, quote observation, inline `IntentRecord` emission, and `ReportRecord` receipt.

* SDK and example implementation: [registered.rs](../../crates/tickwire-strategy-sdk/src/registered.rs), [bootstrap.rs](../../crates/tickwire-strategy-sdk/src/bootstrap.rs), [catalog.rs](../../crates/tickwire-strategy-sdk/src/catalog.rs), [lib.rs (example)](../../crates/tickwire-strategy-example/src/lib.rs)
* Integration tests: [example_worker.rs](../../crates/tickwire-strategy-example/tests/example_worker.rs), [microstructure_sdk.rs](../../crates/tickwire-strategy-sdk/tests/microstructure_sdk.rs)
* Archived validation evidence: [strategy-parameters.json](../evidence/20261029/strategy-parameters.json)
* Related specifications: [Strategy input consumption](strategy-input-consumption.md), [Instrument catalog](instrument-catalog.md), [Configuration candidates](config-candidate.md)
