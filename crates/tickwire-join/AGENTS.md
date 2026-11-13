# tickwire-join

`tickwire-join` coordinates live topic onboarding, catch-up activation, epoch freeze, and quiesced detachment between a
market publisher (`JoinOwner<M>`) and a subscriber (`JoinConsumer<B>`) over authenticated control sessions
(`tickwire-control`), `SCM_RIGHTS` descriptor frames (`tickwire-control-sys`), sealed snapshots (`tickwire-snapshot`),
and shared-memory banks (`tickwire-shm`). [src/lib.rs](src/lib.rs) enforces `#![deny(unsafe_code)]`; only
[src/mapped.rs](src/mapped.rs) may discharge the peer-behavior safety obligations of `Publisher` and `Subscriber`.

## Invariants and Change Guidelines

* **Isolated `unsafe` peer boundary**: Keep all `unsafe` calls (`Publisher::register_pending_cursor` in
  `JoinOwner::receive_cursor` and `Subscriber::attach` in `JoinConsumer::attach`) confined to
  [src/mapped.rs](src/mapped.rs), only after authenticating `Session` peer identity, `OfferPrefix` seals (`0x17` for
  single-writer bank/cursor, `0x0f` for immutable snapshot), `ResourceDescriptor` header hashes, and exact `JoinBody` /
  `TopicRegistry` bindings.
* **Pending baseline retention before snapshot build**: `JoinOwner` ([src/lib.rs](src/lib.rs)) must register the
  subscriber's pending ring gate (`Stage::Reserved` -> `Stage::Prepared`) at `snapshot_seq` before sealing and offering
  the `ImmutableResource` snapshot so concurrent `publish` / `publish_market` batches cannot overwrite slots needed
  between `snapshot_seq` and live catch-up.
* **Monotonic stage and watermark progression**: Advance each entry strictly through
  `Reserved -> Prepared -> Delivered -> Active` (or `Failed` / `Expired` / `Quiescing -> Detached`) under non-decreasing
  `now` timestamps. Activation (`Publisher::activate_join`) requires both bank and snapshot `ResourceAckBody` receipts,
  a registered cursor, and verified `applied >= baseline` watermarks before `readiness_deadline_mono_ns`.
* **Verified detachment**: `JoinOwner::all_detached` returns `true` only after every registered subscription completes
  `QuiesceBody` exchange and reaches `Stage::Detached`, protecting successor ring cutover (`freeze_for_successor`).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/mapped.rs](src/mapped.rs) · [tests/owner.rs](tests/owner.rs) ·
  [tests/flow.rs](tests/flow.rs) · [tests/process_linux.rs](tests/process_linux.rs)
* **Specifications and models**: [join.md](../../specs/implementation/join.md) ·
  [shm-ring.md](../../specs/implementation/shm-ring.md) ·
  [topic-snapshot.md](../../specs/implementation/topic-snapshot.md) ·
  [join_owner](../../tla/execution_models/join_owner/ROADMAP.md)

```sh
cargo test --locked -p tickwire-join --all-targets
./scripts/check-linux.sh
./scripts/check-tla.sh join_owner
```
