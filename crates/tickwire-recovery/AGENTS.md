# tickwire-recovery

`tickwire-recovery` (`#![forbid(unsafe_code)]`) owns content-addressed immutable recovery artifacts
(`RecoveryManifest`, `DependencyObject`, `HandoverSeal`, `EncodedInputs`), private `VerifiedClosure` capabilities, and
local blob storage (`FileBlobStore`, `FileBlobReader`). Authority implementations publish and fsync strategy inputs,
config pins, command payloads, and account baselines/histories here before returning a `VerifiedClosure` to an
admitting gateway. A `VerifiedClosure` proves every required dependency object is synchronized and hash-verified—it
never grants trading permission on its own.

## Invariants and Change Guidelines

* **Complete dependency graph and DAG validation**: Manifest verification in [src/lib.rs](src/lib.rs) and
  [src/codec.rs](src/codec.rs) enforces required dependency kinds (`1..=12`), optional command (`13`) and paired
  account baseline/history (`14`/`15`), acyclic `(dependent, dependency)` edges, `MAX_OBJECT_BYTES` (64 MiB), and
  `MAX_CLOSURE_BYTES` (256 MiB) before constructing `VerifiedClosure`.
* **Cross-intent binding checks**: Bind every `RecoveryManifest` to its exact `ClosureBinding` (`risk_domain_id`,
  `intent_id`, `intent_exec_epoch`, `business_digest`, `strategy_generation`, `config_revision`, `recovery_object`) so
  a valid manifest or blob from another order cannot satisfy verification (`Error::Conflict`).
* **Append-only `0o700` file store and `O_NOFOLLOW` reads**: `FileBlobStore` and `FileBlobReader` in
  [src/files.rs](src/files.rs) expose no deletion API, check UID and `0o027` permission masks on Unix, open blobs with
  `O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK`, and re-verify SHA-256 digests on every read.
* **Deterministic replay inputs and handover seals**: Preserve canonical framing in [src/inputs.rs](src/inputs.rs) and
  [src/handover.rs](src/handover.rs) so recorded market/parameter inputs and worker handover seals hash identically
  during replay and rebuild.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/codec.rs](src/codec.rs) · [src/files.rs](src/files.rs) ·
  [src/handover.rs](src/handover.rs) · [src/inputs.rs](src/inputs.rs)
* **Integration tests and fixtures**: [tests/closure.rs](tests/closure.rs) ·
  [tests/handover_rebuild.rs](tests/handover_rebuild.rs) · [manifest.hex](tests/manifest.hex)
* **Specifications and models**: [recovery-closure.md](../../specs/implementation/recovery-closure.md) ·
  [account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md) ·
  [recovery_closure](../../tla/execution_models/recovery_closure/ROADMAP.md) ·
  [business_journal](../../tla/execution_models/business_journal/ROADMAP.md)

```sh
cargo test --locked -p tickwire-recovery --all-targets
./scripts/check-tla.sh recovery_closure
```
