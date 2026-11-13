# tickwire-release

`tickwire-release` (`#![forbid(unsafe_code)]`) owns immutable strategy release bundles (`ReleaseManifest`,
`ReleaseBundle`, `Operation`), bounded `TWINP002` replay input logs ([src/replay.rs](src/replay.rs)),
declared-rebuild contracts ([src/rebuild.rs](src/rebuild.rs)), checkpointed account-input verification
([src/account_inputs.rs](src/account_inputs.rs)), and persistent handover completion graphs
([src/closure.rs](src/closure.rs), [src/receipt.rs](src/receipt.rs)). Content and digest validation here
proves historical artifact and input continuity; live worker execution and handover publication remain owned
by `tickwire-strategy-sdk` and `tickwire-strategy-host`.

## Invariants and Change Guidelines

* **Handover seal and WAL parity**: `account_inputs::verify` ([src/account_inputs.rs](src/account_inputs.rs))
  validates `HandoverSeal` digests, reconstructs `VerifiedAccountInputs` (`CanonicalBaseline` and `TWAI0001`
  `CanonicalRecord` callbacks up to `MAX_ACCOUNT_RECORDS`), and requires byte-exact `matches_history`
  agreement against the live account WAL before handover.
* **Bounded closure graph traversal**: Keep `closure` verification ([src/closure.rs](src/closure.rs)) strictly
  bounded (`<= 256` cached objects, `<= 16` recursion depth, `MAX_OBJECT_BYTES`, and `MAX_CLOSURE_BYTES`),
  verifying every SHA-256 object digest on read before trusting `Completion`, `Receipt`, or `HandoverSeal`
  links.
* **Deterministic replay and rebuild installation**: Preserve wire and JSON compatibility
  (`#[serde(deny_unknown_fields)]`, `TWINP002` framing, `MAX_BYTES = 16 MiB`, `MAX_ROWS = 65,536`) across
  [src/replay.rs](src/replay.rs), [src/rebuild.rs](src/rebuild.rs), and atomic directory/file installation
  helpers in [src/files.rs](src/files.rs), [src/replay/install.rs](src/replay/install.rs), and
  [src/rebuild/install.rs](src/rebuild/install.rs), including rollback receipts (`rollback`).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/account_inputs.rs](src/account_inputs.rs) ·
  [src/closure.rs](src/closure.rs) · [src/receipt.rs](src/receipt.rs) · [src/replay.rs](src/replay.rs) ·
  [src/rebuild.rs](src/rebuild.rs) · [src/files.rs](src/files.rs) · [tests/replay.rs](tests/replay.rs) ·
  [tests/retained_installation.rs](tests/retained_installation.rs)
* **Specifications and models**: [recovery-closure.md](../../specs/implementation/recovery-closure.md) ·
  [account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md) ·
  [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [recovery_closure](../../tla/execution_models/recovery_closure/ROADMAP.md)

```sh
cargo test --locked -p tickwire-release --all-targets
./scripts/check-tla.sh recovery_closure
```
