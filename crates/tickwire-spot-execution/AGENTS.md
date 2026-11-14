# tickwire-spot-execution

`tickwire-spot-execution` (`#![forbid(unsafe_code)]`) implements authenticated Bybit V5 and OKX V5 cash-spot REST
request signing (`SpotEncoder`), single-flight and multi-connection pooled gateway sessions (`RestSession`), EWMA
`OrderRouter` + `VenueRateLimiter` routing, synchronous response classification (`decode_reply`), `StorageMode`-aware
private WebSocket stream capture (`private`), background dialer connection handoff (`network::{LiveNetwork,
SocketConnection}`), and UID-bound REST recovery collection (`recovery`, `observe`, and the `tickwire-spot-observe`
binary). `tickwire-spot-runner` drives `RestSession` over an injected `AccountGateway`; strategy scheduling, balance
math, and control leases remain outside this crate.

## Invariants and Change Guidelines

* **Strict cash-spot parameter domain and secret redaction**: `SpotEncoder` in [src/lib.rs](src/lib.rs) accepts only
  `ProductMode::Spot` base-quantity orders (`tdMode: "cash"` on OKX, `category: "spot"` on Bybit) with validated ASCII
  credentials, symbols, and venue-bounded `client_id` lengths (`32` OKX, `36` Bybit); never implement `Debug` or error
  formatting that exposes API secrets or passphrases.
* **Distinct acknowledgement, modification, order-status, fill, and error-classification evidence**: Keep synchronous
  HTTP acknowledgements, in-place `CommandBody::Modify(ModifyOrder)` outcomes (`ModifyReply`, `decode_modify_reply`,
  `record_modify_outcome`), and venue error/terminal reason classification (`SpotIssueKind`, `classify_issue`,
  `classify_order_row_terminal_reason`, `extract_order_row_extras`, `is_http_account_hazard`, `is_venue_rate_limit_code`)
  in [src/reports.rs](src/reports.rs) and [src/session.rs](src/session.rs) separate from trade-level fills in
  [src/fills.rs](src/fills.rs) and private stream frames in [src/private.rs](src/private.rs); encode Bybit V5
  `POST /v5/order/amend` (`X-BAPI-*` HMAC-SHA256) and OKX V5 `POST /api/v5/trade/amend-order` (`OK-ACCESS-*` Base64
  HMAC-SHA256) in [src/lib.rs](src/lib.rs), default unknown venue codes conservatively to `SpotIssueKind::Ambiguous`,
  preserve `per_order_doubt_isolation() == false` by default in unpooled mode, record `VenueEventBody::Rejected`
  (`record_rejected_new`) only on `DeterministicReject` for `New` orders, emit `VenueEventBody::Modified` or
  `VenueEventBody::ModifyRejected` without freezing `Account` on deterministic `Modify` responses, isolate ambiguous
  `Modify` outcomes under `DoubtReason::AmendAmbiguous`, and enforce `writer_epoch > durable_floor` when constructing
  `RestSession`.
* **Multi-connection exclusive HTTP/1.1 pool and `Place`/`Modify`/`Cancel`/`Query` isolated `VenueRateLimiter`**: Preserve
  100% single-connection `RestSession` semantics when `gateway.has_connection_pool() == false`. When pooled via
  `with_connection_pool` / `enable_connection_pool` in [src/session.rs](src/session.rs), enforce `SendSlot::Exclusive`
  per slot (`0..MAX_ORDER_CONNECTIONS`), derive non-zero `symbol_rate_key(instrument_id)` (auto-registering symbols on
  `OkxSpot`), debit `CommandBody::Modify` from the order/amend quota (`allow_modify(..., counts_as_order: true)`) only
  **after** `AccountGateway` admission succeeds (never burning tokens on local `CrossesBook`, `InsufficientAsset`,
  `TooManyPendingActions`, or `NoChange` refusals, and never consuming cancel-only reserve), return
  `SessionError::RateLimited(RateReject)` **before** `gateway.submit_on_slot` when local `Place`, `Modify`, `Cancel`, or
  `Query` budgets are full, correlate concurrent completions via `poll_tagged`, record microsecond RTTs via
  `note_slot_sample(..., SampleKind::Order, sample_us)`, isolate single-slot drops via `drop_broken_slot_isolated`, and
  translate HTTP `429` / Bybit `10006` / OKX `50011` on multi-IP pooled slots into per-IP `note_ip_cooldown` +
  single-intent `in_doubt` without freezing `Account` (while HTTP `401`/`403`/`418` and auth hazards still freeze
  `Account` immediately).
* **Signed off-hot-path REST `GET` builders**: Reuse `build_order_status_get`, `build_order_trades_get`, and `decode_fill_row`
  in [src/lib.rs](src/lib.rs) and [src/recovery.rs](src/recovery.rs) for `AsyncRestQuerier` status and trade catch-up
  without duplicating HMAC signing or native row validation.
* **`StorageMode`-aware private capture and bounded `InMemory` windowing**: In
  [src/private/capture.rs](src/private/capture.rs) and [src/private/stream.rs](src/private/stream.rs), `StreamConfig`
  selects `StorageMode::InMemory` (`Journal<InMemoryStore>` + `InMemoryBlobStore` with zero disk directories/syscalls
  and bounded ring trimming of consumed descriptors/frame blobs past `max_descriptors`), `StorageMode::MmapAsync`
  (`Journal<MmapAsyncStore>` + `AsyncFileBlobStore`), or `StorageMode::LocalDurable` (strict `0o077` owner-only
  directory checks with `Journal<FileStore>` + `FileBlobStore`).
* **Off-hot-path background reconnects and calibrated venue timestamps**: Use `LiveNetwork` (`attach_pool_slot`,
  `create_pool_baton`) and `SocketConnection` in [src/network.rs](src/network.rs) to integrate optional `Dialer` +
  `DialBaton` pre-warmed TCP+TLS reconnects across `1..=N` local source IPs off the `AccountRunner` hot path, and supply
  calibrated UTC milliseconds via the injected `time` closure in `SpotEncoder` and `network::venue_time`, failing closed
  when time calibration is unavailable.
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
