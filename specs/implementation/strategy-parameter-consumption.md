# Strategy Parameter Validation and First-Intent Consumption Gate

`HostConfig.projection_parameters` (`tickwire-app::hosted::parameters`) closes the gap between an approved global `strategy.params` value in a deployment projection and the private parameter bytes actually parsed by a strategy worker's factory at runtime. While the global `ProjectionObject` revision and digest identify the cluster-level configuration decision, `bootstrap.factory` (`FactoryBinding`) identifies the worker's private `revision` and raw configuration file bytes. When this optional binding is enabled, `App` validates both documents offline before process creation and withholds the worker's first queued trading intent until a live host checkpoint query (`FreshObservation::configuration()`) proves that the running worker recorded that exact `VersionedBytes` revision and SHA-256 at its current merged cut.

Omitting `projection_parameters` preserves legacy unconstrained startup for workers without parameter projections. When enabled alongside `projection_launch` or `projection_inputs`, every active gate must reference the identical `ProjectionObject` and `Scope`. `ParametersProof` records one verified runtime observation for a specific `(logical_strategy, instance, generation)`; global configuration materialization (`Applied` / `Effective` receipts) and order-send risk authority remain governed by their own owners.

## State Model and Bounds

`parameters::Config` (`#[serde(deny_unknown_fields)]`) binds `object_file`, `profile::Binding`, `profile::Scope`, `schema_file`, and `timeout_ns`. It applies to the initial launch of an active registered strategy worker built with mode-2 checkpoint-query support.

| Artifact or Parameter | Validation Rule / Enforced Bound |
| --- | --- |
| `ProjectionObject` & startup binding | Verified whole-object digest (`<= 4 MiB`), matching cluster, namespace, host, process, `role == strategy`, logical strategy ID, exact `(instance, generation)`, local bootstrap channel identities, and sealed executable `build_hash`. |
| Parameter JSON Schema (`schema_file`) | Raw file SHA-256 must equal `strategy.parameter_schema_ref`; validated as offline JSON Schema Draft 2020-12 (`<= 1 MiB`, `<= 32` container nesting levels, same-document `$ref` only, zero external retrieval, linear-time regex engine with bounded compiled/DFA caches). |
| Private factory file (`FactoryBinding`) | Read from absolute `factory.configuration_file`; raw size and SHA-256 verified against `factory.configuration` (`<= 2 MiB`); `factory.revision < 2^63`. |
| Parameter document (`validate_parameter_document`) | Strict JSON parser rejects duplicate keys (including `\u` escaped collisions), floating-point numbers, integers outside `±(2^53 - 1)`, trailing bytes, and excessive depth; top-level JSON object requires `1..=256` properties and `<= 16` nesting levels. |
| Canonical value equivalence | Canonicalized `Candidate` of the private file must equal `projection.payload()["strategy"]["params"]` structurally (allowing benign whitespace or key-order differences in the raw file read by the factory while pinning both canonical `parameter_hash` and raw `private_object`). |
| Gate timing | Query `timeout_ns` bounded at `<= 30 s` (`30,000,000,000` ns); internal gate poll interval `>= 10 ms` (`10,000,000` ns). |

## Core Invariants

* **End-to-end SDK -> checkpoint -> host chain**: Inside the worker, the qualified SDK passes the exact `VersionedBytes` (`revision` and raw `bytes`) to `Registration::configure`, verifies the factory's readback, checks replay registration after strategy construction, and commits the resulting `VersionedBytes` into `RuntimeState.config`. On the host side, `FreshObservation::configuration()` refuses to read arbitrary files on disk: it decodes the `HandoverSeal` named by the fresh `CHECKPOINT_SEALED` reply, verifies the raw checkpoint and retained merged input stream, checks `source` (`instance`, `generation`, `logical_strategy`, `binary_digest == image`, and logical `ClockDomain` `boot_id`/`generation` alongside the independently sampled kernel `boot_id`), and confirms that the checkpoint's business payload (`raw[96..]`) matches `current.business` byte-for-byte before extracting `current.runtime.config`.
* **Rejection of stale or unmerged checkpoints**: Because every query issues a unique challenge bound to the live child's PID, `start_ticks`, kernel `boot_id`, `control_epoch`, and monotonically advancing `seal_sequence`, neither an old startup checkpoint nor an unrelated file sharing the same parameter SHA-256 can satisfy `Prepared::observe`. A legacy closure with zero merged input rows (`has_merged == false`) returns `configuration = None` (which `Prepared::observe` rejects), whereas any partial or malformed merged history fails `retained::decode` directly.

## Operations and Failure Semantics

* **Nonblocking intent withholding and timeout termination**: While waiting for the account report baseline, a busy host control lane, or the worker's `CHECKPOINT_SEALED` reply, `App` continues pumping market and account inputs into the worker and advances the internal query at most once every `10 ms`. Outgoing intents remain queued inside `App` and cannot reach the intent ring or execution gateway. Immediately after disk and `/proc` verification completes, `App` resamples the monotonic clock against the original absolute deadline (`<= 30 s`); any timeout or mismatch closes the gate and terminates and reaps the managed child.
* **Isolation from external inbox queries and joint proof publication**: Diagnostic queries arriving through `ObservationInbox` cannot consume, cancel, or interfere with `App`'s internal startup gate. When `projection_inputs` is also active, both verifiers evaluate the identical `Arc<FreshObservation>` under the earlier of their two deadlines and publish `ParametersProof` (`AppStrategy::projection_parameters(target)`) and `InputsProof` atomically before releasing the first queued intent.

## Verification and References

```sh
cargo test --locked -p tickwire-app --test parameters_process
cargo test --locked -p tickwire-strategy-host
```

Six archived Linux multi-process acceptance tests drove an independent counting strategy worker against an authenticated market-data `JOIN` server, covering optional unconstrained compatibility, pre-spawn rejection of invalid schemas/values, live private parameter attestation, behavior changes across counting and minimum-bid parameter revisions, and intent withholding through deadline expiry and child reaping.

* Host and app implementation: [parameters.rs](../../crates/tickwire-app/src/hosted/parameters.rs), [configuration.rs](../../crates/tickwire-strategy-host/src/observation/configuration.rs)
* Process integration tests: [parameters_process.rs](../../crates/tickwire-app/tests/parameters_process.rs)
* Archived validation evidence: [strategy-parameters.json](../evidence/20261029/strategy-parameters.json)
* Related specifications: [Runtime observation](runtime-observation.md), [Strategy input consumption](strategy-input-consumption.md), [Supervisor process](supervisor-process.md), [Configuration candidates](config-candidate.md)
