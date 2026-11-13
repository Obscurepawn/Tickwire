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
