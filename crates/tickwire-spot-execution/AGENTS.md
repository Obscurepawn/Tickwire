# tickwire-spot-execution

`tickwire-spot-execution` (`#![forbid(unsafe_code)]`) implements authenticated Bybit V5 and OKX V5 cash-spot REST
request signing (`SpotEncoder`), single-flight gateway sessions (`RestSession`), synchronous response classification
(`decode_reply`), `StorageMode`-aware private WebSocket stream capture (`private`), background dialer connection
handoff (`network::{LiveNetwork, SocketConnection}`), and UID-bound REST recovery collection (`recovery`, `observe`,
and the `tickwire-spot-observe` binary). `tickwire-spot-runner` drives `RestSession` over an injected `AccountGateway`;
strategy scheduling, balance math, and control leases remain outside this crate.

## Invariants and Change Guidelines

* **Strict cash-spot parameter domain and secret redaction**: `SpotEncoder` in [src/lib.rs](src/lib.rs) accepts only
  `ProductMode::Spot` base-quantity orders (`tdMode: "cash"` on OKX, `category: "spot"` on Bybit) with validated ASCII
  credentials, symbols, and venue-bounded `client_id` lengths (`32` OKX, `36` Bybit); never implement `Debug` or error
  formatting that exposes API secrets or passphrases.
* **Distinct acknowledgement, order-status, fill, and error-classification evidence**: Keep synchronous HTTP
  acknowledgements and venue error classification (`SpotIssueKind`, `classify_issue`, `is_http_account_hazard`) in
  [src/reports.rs](src/reports.rs) and [src/session.rs](src/session.rs) separate from trade-level fills in
  [src/fills.rs](src/fills.rs) and private stream frames in [src/private.rs](src/private.rs); default unknown venue
  codes conservatively to `SpotIssueKind::Ambiguous`, preserve `per_order_doubt_isolation() == false` by default, record
  `VenueEventBody::Rejected` (`record_rejected_new`) only on `DeterministicReject` for `New` orders, and enforce
  `writer_epoch > durable_floor` when constructing `RestSession`.
* **Signed off-hot-path REST `GET` builders**: Reuse `build_order_status_get`, `build_order_trades_get`, and `decode_fill_row`
  in [src/lib.rs](src/lib.rs) and [src/recovery.rs](src/recovery.rs) for `AsyncRestQuerier` status and trade catch-up
  without duplicating HMAC signing or native row validation.
* **`StorageMode`-aware private capture and bounded `InMemory` windowing**: In
  [src/private/capture.rs](src/private/capture.rs) and [src/private/stream.rs](src/private/stream.rs), `StreamConfig`
  selects `StorageMode::InMemory` (`Journal<InMemoryStore>` + `InMemoryBlobStore` with zero disk directories/syscalls
  and bounded ring trimming of consumed descriptors/frame blobs past `max_descriptors`), `StorageMode::MmapAsync`
  (`Journal<MmapAsyncStore>` + `AsyncFileBlobStore`), or `StorageMode::LocalDurable` (strict `0o077` owner-only
  directory checks with `Journal<FileStore>` + `FileBlobStore`).
* **Off-hot-path background reconnects and calibrated venue timestamps**: Use `LiveNetwork` and `SocketConnection` in
  [src/network.rs](src/network.rs) to integrate optional `Dialer` + `DialBaton` pre-warmed TCP+TLS reconnects off the
  `AccountRunner` hot path, and supply calibrated UTC milliseconds via the injected `time` closure in `SpotEncoder` and
  `network::venue_time`, failing closed when time calibration is unavailable.
* **Replayable UID-bound REST recovery transcripts**: `RestReader` in [src/recovery.rs](src/recovery.rs) and `observe`
  in [src/observe.rs](src/observe.rs) execute an identical paging and cashflow verification plan on live collection
  and local replay, refusing unsupported margin, transfer, or non-cash activity (`src/recovery/cash_flows.rs`,
  `src/recovery/profile.rs`).

## Verification and Specifications

* **Source and binaries**: [src/lib.rs](src/lib.rs) · [src/session.rs](src/session.rs) ·
  [src/reports.rs](src/reports.rs) · [src/fills.rs](src/fills.rs) · [src/private.rs](src/private.rs) ·
  [src/network.rs](src/network.rs) · [src/recovery.rs](src/recovery.rs) · [src/observe.rs](src/observe.rs) ·
  [src/bin/tickwire-spot-observe.rs](src/bin/tickwire-spot-observe.rs)
* **Integration tests**: [tests/protocol.rs](tests/protocol.rs) · [tests/session.rs](tests/session.rs) ·
  [tests/private.rs](tests/private.rs) · [tests/recovery.rs](tests/recovery.rs) ·
  [tests/observations.rs](tests/observations.rs)
* **Specifications and models**: [spot-execution.md](../../specs/implementation/spot-execution.md) ·
  [spot-rest-recovery.md](../../specs/implementation/spot-rest-recovery.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md) ·
  [strategy_intake](../../tla/execution_models/strategy_intake/ROADMAP.md)

```sh
cargo test --locked -p tickwire-spot-execution --all-targets
./scripts/check-tla.sh spot_roundtrip
```
