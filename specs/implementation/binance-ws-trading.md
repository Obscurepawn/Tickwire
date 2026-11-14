# Binance WebSocket Order-Entry Owner and Dual-WAL Recovery

`tickwire-binance-account::rest::RestOwner` owns trading-account ingress and outbound command coordination across both REST and WebSocket order-entry transports. When WebSocket trading is enabled (`trading_websocket` paired with `network.trading_websocket`; omitting both selects REST order entry), `RestOwner` drives a dedicated order-entry WebSocket connection alongside a durable request-correlation binding in the `TWAI0001` account-input journal, while `network.rest` remains the independent read-only reconciliation endpoint.

`AccountIngress` and the WebSocket session owner (`ws::owner`) own the input-journal records, live-only `Rc<OwnerState>` capabilities (`DurableWsSession`, `DurableWsBinding`), and raw response quarantine, while `AccountGateway` owns the account WAL, risk reservations, and first-byte transport gate.

## Durable Journal Records and Wire Identity

Five `TWAI0001` input-journal record tags (`16..=20`, validated for exact length and canonical encoding) capture the complete WebSocket trading lifecycle:

| Input Record Tag | Variant | Durable Contents & Role |
| --- | --- | --- |
| `16` | `WsSession` | Persists `SessionIdentity` (`input_stream`, `account_stream`, `baseline_digest`, `Scope`, `Product`, `execution_instance`, `writer_epoch`, `connection_epoch`, `credential_ref_digest`) *before* opening the socket. |
| `17` | `WsBinding` | Binds `SessionRecord`, input `sequence`, `CanonicalCommand`, `account_sequence`, `record_digest`, `PhysicalFrontier`, 128-byte account history seal, and `SignedWire` (`<= 4,096` `MAX_REQUEST_BINDINGS`). |
| `18` | `WsResponse` | Records raw WebSocket reply bytes, match/quarantine decision, and correlation metadata *before* deriving account events. |
| `19` | `WsEffect` | Indexes `(response_seq, effect_index)` linking a `WsResponse` to each derived account `IngressEvent`. |
| `20` | `WsSessionEvent` | Records connection upgrade, close, authentication/clock fault, or lifecycle transitions. |

Each outbound WebSocket request carries a deterministic **57-byte** printable ASCII `WsRequestId` (`<= 64` `MAX_REQUEST_ID_BYTES`) that reversibly encodes `(input_stream, writer_epoch, connection_epoch, binding_sequence)`, allowing incoming replies to be correlated directly to their durable `WsBinding` without heuristic lookup.

## Indexed Order Registry and Windowed Eviction

`AccountIngress` encapsulates admitted order registrations inside `OrderRegistry` ([`src/registry.rs`](../../crates/tickwire-binance-account/src/registry.rs)), maintaining primary `IntentKey -> Registration` storage alongside secondary `BTreeMap` indices over local 16-byte `order_id`, `(instrument_id, ClientId)`, `(instrument_id, VenueOrderId)`, list IDs, and algo IDs:

* **O(log N) private-frame correlation without full-registry cloning**: `ingest_private` ([`src/runtime.rs`](../../crates/tickwire-binance-account/src/runtime.rs)) resolves incoming order, list, and conditional/algo executions via secondary indices in `O(log N)` time rather than scanning all registrations linearly, and validates/applies newly learned venue identifiers incrementally (`check_learn` before journal append, `apply_learn` after append) without cloning `OrderRegistry` on each private WebSocket frame.
* **Windowed retirement and prefix compaction (`retention_window_ns`)**: Both `AccountIngress` and `RestOwner` default `retention_window_ns` to `Some(DEFAULT_RETENTION_WINDOW_NS)` (`86,400,000,000,000` ns / `24 h`, configurable via `set_retention_window_ns`). Retired `IntentKey`s whose `Account::intent_retired_mono_ns` has aged past `retention_window_ns` (and are no longer active in `Account` or `AccountGateway`) are lazily evicted from `OrderRegistry`, `ws_bindings`, and—in `InMemory` / `MmapAsync` deployments—the consumed in-memory `AccountIngress::records` prefix (`evicted_prefix`), while preserving active orders, `Record::Config`, and exact `max_registrations` / `max_records` enforcement inside any single window.

## Core Invariants

* **Live-only capability isolation (no replay re-minting)**: `DurableWsSession` and `DurableWsBinding` hold a private `Rc<OwnerState>` tracking `alive` and active `connection_epoch`. Dropping the owner or advancing/revoking the session flips `alive = false`, immediately invalidating stale plans and bindings (`is_current() == false`). Decoding historical `TWAI0001` records during recovery yields read-only `BindingEvidence`, never a live `DurableWsBinding` capable of transmitting bytes.
* **Durable binding outside admission closure, before first byte**: An admitted intent enters `AwaitingCommit` (holding its account risk reservation) once its admission closure is synced to the account WAL. Because `WsBinding` is appended to the input WAL *after* account admission, its input position lies outside the admission's original recovery closure; however, `GateAuthority` holds `CommitState::Pending` until `DurableWsBinding` is synced to disk and verified against the cached `SignedWire`, SHA-256 masked-frame hash, and exact account `AppendReceipt` **before** the transport accepts the first plaintext byte.
* **Frame-header/mask bytes count as `MayHaveSent`**: `AccountGateway` may record a verified `Aborted` account record and release an order's reservation *only* upon local proof from the transport that zero application bytes entered TLS (`accepted_bytes == 0`). If the TLS writer accepts even a single byte of the WebSocket frame header or 4-byte client mask, the send enters `MayHaveSent`; neither a subsequent disconnect, a mismatched reply ID, nor an unresolved crash recovery can treat that intent as unsent.
* **Input WAL precedes account WAL on response**: Every matched WebSocket response and its derived `WsEffect` markers are synced to the input WAL *before* appending the corresponding `IngressEvent` records to the account WAL. Order replies reporting cumulative executed quantity produce order-status observations only—individual fills, fees, and rebates require dedicated trade/execution evidence.

## Session Control, Responses, and Dual-WAL Recovery

`ControlAction` permits (`Connect`, `Upgrade`, `Ping`, `Pong`, `Close`) charge the shared quota endpoint ([binance-quota.md](binance-quota.md)), and the owner validates the HTTP `101` WebSocket `Upgrade` before accepting order submissions. Outbound `Pong` and `Close` frames interleave only at complete application-frame boundaries; if a `Close` cannot be sent cleanly at a boundary, the socket drops while outstanding in-flight orders keep their reservations intact.

When processing inbound frames, a second reply carrying an already-matched `WsRequestId` is recorded as a duplicate without re-applying effects, while unexpected request IDs, malformed JSON, or incomplete envelopes are persisted raw and quarantined. Venue `429`/`418` replies immediately freeze the shared quota consumer, and authentication or clock-skew errors halt the session.

During crash recovery across the paired input and account WALs:

1. Each `WsBinding` is cross-checked against its referenced account admission receipt, `PhysicalFrontier`, and 128-byte history seal; any conflicting duplicate binding halts the owner (`Error::Conflict`).
2. Every persisted `WsResponse` is reparsed from its raw bytes and checked against its recorded effects; if a crash occurred after syncing `WsResponse` to the input WAL but before syncing all derived `IngressEvent` records to the account WAL, paired recovery deterministically repairs the torn tail by appending only the missing account records under their original `InputKey`s.
3. Intents recovered with unknown wire outcomes retain their reservations without a live `CommittedAdmission` or old socket. Only a verified `Reactivated` account record under a new `writer_epoch` can open a fresh `WsSession`, and doing so grants zero authority to replay historical frames.

## Per-Order Doubt Isolation, Deterministic Order Rejections, and `PerOrderQueryDriver`

By default (`gateway.per_order_doubt_isolation() == false`), any non-`Observed` outbound command response freezes `Account` until whole-account reconciliation completes. Calling `RestOwner::with_per_order_query_desk(QueryConfig)` or `enable_per_order_query_desk(QueryConfig)` activates per-order doubt isolation (`gateway.set_per_order_doubt_isolation(true)`) together with a preallocated `tickwire_account::QueryDesk`:

* **Deterministic `VenueEventBody::Rejected` on single-order creation rejection**: Documented Binance Spot and USDⓈ-M deterministic rejection codes—including `-2010`, `-2019` (insufficient margin), `-2021` (would immediately trigger when HTTP status `!= 409`; `409` remains `FailureKind::Partial`), `-2038`, `-4061` (position side mismatch), `-4164` (`MIN_NOTIONAL`), and `-5022` (GTX post-only taker rejection)—derive a byte-exact `CanonicalEvent` with `VenueEventBody::Rejected { client_id: Some(spec.client_id), venue_code, message }` (`event_id = SHA256(event_domain || command_digest || epoch_le || raw || 0u64_le)`). Both `validate_rest` and `validate_ws_response` / `verify_ws_response` re-verify raw derivation on replay, while `Account::apply_event` marks the unconfirmed order `uncreated = true` and `fees_closed = true`, releasing its reservation and shadow hold without freezing `Account`.
* **Category A (single-order isolation) vs. Category B (account-wide freeze)**:
  * **Category B** hazards—`FailureKind::Banned` (`418`), `FailureKind::RateLimited` (`429`, `-1003`, `-1015`), `FailureKind::Authentication` (`401`, `-1002`, `-2014`, `-2015`, `-1022`), `FailureKind::ClockSkew` (`-1021`), private-stream `GapNotice` / `QuarantineReason`, `OrderStatus::Unknown` or `Account::apply_event` errors, and multi-leg `SpotList` / `Algo` / `CancelReplace` ambiguities—unconditionally call `gateway.freeze_account()`.
  * **Category A** single-order ambiguities—`FailureKind::Unknown` (`-1007`, `-1000`, `-1001`, `-1006`, `408`, non-rejected `5xx`), `FailureKind::NotFound` (`-2011` on `Cancel`), socket drop / response timeout (`gateway.drop_broken_transport_isolated()` + `RestOwner::install_transport_connection`), or `200 OK` `RestDisposition::NeedsReconciliation` with `executedQty > 0` before trade fills arrive—isolate only that single order via `gateway.isolate_intent_in_doubt(request, reason)` (`PlaceAmbiguous`, `CancelAmbiguous`, `AmendAmbiguous`, or `FillCatchup`) and enqueue its `AskKind` into `QueryDesk` while sibling strategies continue submitting and filling on an unfrozen `Account`.
* **Off-hot-path resolution via `PerOrderQueryDriver` (`src/query_desk_driver.rs`)**: Drives `RestOwner`'s `QueryDesk` over `tickwire_transport::rest_querier::AsyncRestQuerier` on a dedicated keep-alive HTTPS connection without blocking outbound order entry:
  * Skips due asks with zero wire calls when private-stream frames have already confirmed, filled, or closed the order.
  * Charges quota and counts issued queries only (`on_not_sent` when quota or queue capacity declines; `on_transport_failure` on `SendFail` / `ReadFail` / `Timeout` / HTTP `5xx`, escalating to `gateway.freeze_account()` upon `QueryResolution::FreezeAccount` or Category B HTTP errors).
  * On `-2013` (`FailureKind::NotFound`): retries within `not_found_grace_ns`; once `not_found_grace_ns` elapses for an unconfirmed order (`QueryResolution::CloseUncreated`), persists the `-2013` `OrderQuery` and its deterministic `VenueEventBody::Rejected` event to close the uncreated order and release its reservation without freezing; if previously confirmed, transitions to `AskKind::Trades` (`FetchTradesBeforeClose`).
  * On `200 OK` `OrderStatus` and `OrderTrades`: persists raw HTTP responses and derived events to the paired WALs without freezing, fetches missing trade pages whenever `cumulative > filled` *before* recording `RecoveryRecord::FeesReconciled`, and clears `in_doubt` once fills and terminal fees reconcile.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-account
```

Local Spot and USD-M TLS test fixtures verify Ed25519/HMAC signing, 57-byte `WsRequestId` round-tripping, zero-byte abort vs partial-mask `MayHaveSent` reservation retention, duplicate/quarantined replies, and deterministic dual-WAL repair when a crash interrupts response persistence between the input and account journals.

* [Trading account owner (`RestOwner`)](../../crates/tickwire-binance-account/src/rest.rs)
* [Durable WebSocket session, 57-byte request ID, and `DurableWsBinding`](../../crates/tickwire-binance-account/src/ws.rs)
* [`TWAI0001` input-journal record codec](../../crates/tickwire-binance-account/src/codec.rs)
* [WebSocket session state owner](../../crates/tickwire-binance-account/src/ws/owner.rs)
* [Upgrade and frame-boundary lifecycle](../../crates/tickwire-binance-account/src/ws/lifecycle.rs)
* [Response correlation, duplicate handling, and quarantine](../../crates/tickwire-binance-account/src/ws/response.rs)
* [Input-to-account effect persistence](../../crates/tickwire-binance-account/src/rest/persistence.rs)
* [Paired input/account WAL recovery](../../crates/tickwire-binance-account/src/lib.rs)
* [Account gateway commit, abort, and reactivation](../../crates/tickwire-account-gateway/src/lib.rs)
* [Gateway WebSocket frame transport](../../crates/tickwire-account-gateway/src/websocket.rs)
* [Deployment configuration assembly](../../crates/tickwire-runtime/src/files.rs)
* [Account structure definitions](../../crates/tickwire-account/src/structures.rs)
* [WebSocket trading and dual-WAL recovery tests](../../crates/tickwire-binance-account/src/ws/tests.rs)
