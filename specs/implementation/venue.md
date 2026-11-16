# Canonical Trading Commands, Stateless ORS Domain Model, and Venue Events

`tickwire-venue` defines the immutable wire representations, SHA-256 content identities, structural validators, and stateless Order Routing System (ORS) domain primitives for trading commands (`CanonicalCommand`), normalized exchange observations (`CanonicalEvent`, `CanonicalAccountInput`), multi-product routing (`Product`, `OrderRoute`, `AssetKey`), execution reports (`FrontAck`, `Fill`), and terminal/refusal classification (`TerminalReason`). Strategy workers, stateless ORS gateways, and venue execution adapters share this crate so every subsystem hashes and interprets the exact same canonical CBOR byte sequence without local write-ahead logs (WALs) or recovery-closure files. `tickwire-venue` enforces `#![forbid(unsafe_code)]` and exact fixed-point integer arithmetic (`Decimal` `[coefficient, exponent]`, zero floating point); venue protocol signing belongs to execution adapters, and live balance, margin, and strategy authorization enforcement belong to `Account` and the ORS session manager.

Separating mutable draft construction (`Command`, `VenueEvent`) from sealed canonical values (`CanonicalCommand`, `CanonicalEvent`) guarantees that neither an unvalidated struct nor a non-canonical CBOR payload can obtain a `Hash` digest or cross process boundaries.

## Encoding, Identity, and Stateless ORS Domain Model

`Command` is an editable draft, whereas `CanonicalCommand` privately owns the validated `Command`, its canonical CBOR byte slice, and its SHA-256 `digest`, exposing read-only accessors. Both `CanonicalCommand::new` and `CanonicalCommand::decode` (and the analogous `CanonicalEvent` constructors) run full semantic validation and verify that re-encoding produces the exact input bytes.

```text
command = ["tickwire.venue-command", 1, CommandContext, CommandBody]
event   = ["tickwire.venue-event", version, EventIdentity, VenueEventBody]
digest  = SHA256(complete top-level encoding)
```

| Envelope / Component | Bound or Versioning Rule | Semantic Scope |
| --- | --- | --- |
| Payload size | `MAX_CANONICAL_BYTES = 16,384` (16 KiB) | Enforced prior to allocation and after serialization; string and array lengths check capacity before allocating. |
| `CommandContext` | Schema version `1` | Binds `risk_domain_id`, `venue`, `environment`, `account`, `product`, `intent_id`, `intent_exec_epoch`, `strategy_generation`, `config_revision`, `deadline`, and legacy opaque `recovery_object` (`[0u8; 32]` on stateless paths), plus strategy lineage (`logical_strategy_id`, `instance_id`, `intent_seq`, `authorization_epoch`, policy/config versions, `created_mono_ns`, economic intent, hedge group/leg) and market trigger references. |
| `Product` & multi-product routing | Tags `1..=8`; `OrderRoute { product_mask }`; `AssetKey { wallet_id, asset_id }` | Covers `Spot` (`1`), `LinearPerpetual` (`2`, alias `LinearPerp`), `InversePerpetual` (`3`, alias `InversePerp`), `Option` (`4`, alias `Options`), `LinearFuture` (`5`, alias `LinearFutures`), `InverseFuture` (`6`, alias `InverseFutures`), `UsdM` (`7`, USD-margined linear contracts), and `CoinM` (`8`, coin-margined inverse contracts). `AssetKey { wallet_id: u16, asset_id: u64 }` partitions spot vs. derivative collateral pools within one master account, while `OrderRoute` (`product_mask`, `colo`, `ready`, `ema_score_us`) and `pick_order_route` select low-latency connections with a `9/8` integer EWMA hysteresis margin (`product_mask == 0` serves all products). |
| `TimeInForce` | Tags `1..=6` (`Gtc`, `Ioc`, `Fok`, `PostOnly`, `Gtd`, `Rpi`) | Supports Good-Till-Cancel (`Gtc`), Immediate-Or-Cancel (`Ioc`), Fill-Or-Kill (`Fok`), maker-only (`PostOnly`), Good-Till-Date (`Gtd { expire_utc_ns }`), and Retail Price Improvement (`Rpi`, tag `6`, valid on resting limit orders without iceberg or peg offsets). |
| `FrontAck` & `Fill` | Stateless ORS execution facts | `FrontAck` captures an exchange gateway front-acknowledgement (`ors_order_id`, `strat_id`, `account_ref`, `order_seqnum`, `orig_seqnum`, `client_id`, `venue_order_id`, `received_mono_ns`) distinct from order-book acceptance. `Fill` captures individual execution fills (`ors_order_id`, `strat_id`, `account_ref`, `order_seqnum`, `exec_ref`, `trade_id`, `side`, `liquidity`, `last_quantity`, `last_price`, `cumulative_filled`, `cumulative_projected`, `fee: Option<AssetDelta>`, `amendment: bool`, `projected: bool`, `received_mono_ns`); when `amendment == true`, the fill updates post-trade fee/rebate economics (`fee.is_some()`, `last_quantity == 0` allowed) without double-counting executed quantity or coexisting with `projected == true`. |
| `VenueEvent` versions `1..=5` | Classic lifecycle (`1`), conditional structures (`2`), spot keep-priority amend (`3`), lifecycle-free trade rows (`4`), native `Modified` / `ModifyRejected` (`5`) | Preserves deterministic CBOR wire compatibility across all normalized venue observations (`Order`, `Fill`, `Fee`, `Structure`, `Amendment`, `AmendmentRejected`, `Modified`, `ModifyRejected`). |

### Command Bodies and Multi-Leg Structures

Single-order specs capture `ClientId`, `Side`, `OrderType`, quantity unit (including quote-asset quantities on ordinary spot market buys and sells), limit price, `TimeInForce` (including `Rpi`), `PositionSide`, trading/margin modes, `reduce_only`, self-trade prevention, trigger references, trailing terms (basis points for spot; percentage units plus explicit working-price source for derivatives), and iceberg quantities. Order-management variants (`Cancel`, `CancelList`, `CancelAlgo`, `Query`, `AmendKeepPriority`, `Modify`, `CancelReplace`) preserve the target's original identity, management/owner revision, and execution epoch while encoding replacement parameters explicitly.

Multi-leg conditional commands carry a dedicated parent identity and enforce deterministic leg ordering:

* **Spot OCO**: `[above, below]`
* **Spot OTO**: `[working, pending]`
* **Spot OTOCO**: `[working, pendingAbove, pendingBelow]`

The `CommandBody` enum also models supported spot extensions explicitly; a command can be canonically valid in `tickwire-venue` even when a particular gateway profile declines to execute it.

### Self-Describing Client Order Identity (`SelfDescribingClientId`)

To allow cold stateless Pod restarts and exchange-only recovery to reconstruct strategy attribution, execution epoch, intent sequence floors, and internal order UUIDs directly from authenticated `Account + OpenOrders` snapshots—without prior local WAL files or origin alias tables—`tickwire-venue::client_id` defines a fixed-width, domain-authenticated 32-character `[0-9a-z]` `ClientId` encoding (`SelfDescribingClientId`):

```text
wire = "tw1" || base36_lower_29(146_bit_payload)   // 32 ASCII [0-9a-z] characters
```

The 29-character payload uses lowercase base-36 (`0123456789abcdefghijklmnopqrstuvwxyz`, $2^{146} < 36^{29}$), decoded via exact 3-limb `[u64; 3]` integer arithmetic (`u128` carries, rejecting base-36 values $\ge 2^{146}$ as `External`), satisfying OKX `clOrdId` (`1..=32` `[0-9a-zA-Z]`), Bybit `orderLinkId` (`1..=36`), and Binance Spot/USD-M/COIN-M `newClientOrderId` (`1..=36`) simultaneously:

| Bit Slice (MSB -> LSB) | Field | Width / Domain | Purpose |
| --- | --- | --- | --- |
| `145..=130` | `logical_strategy_id` | `16` bits (`1..=65,535`; ORS `strat_id` `0..=254` maps via `from_ors_order` / `ors_strat_id` as `strat_id + 1`) | Recovers originating strategy ownership across `Account::MAX_STRATEGIES` (`1,024`) and ORS `strat_id` (`0..=254`, rejecting `255 = ALL_STRATS`). |
| `129..=126` | `leg` | `4` bits (`0..=15`) | Disambiguates single orders (`0`), cancel-replace legs (`1`), multi-leg `SpotList` children (`0..=2`), and parent list/algo IDs (`15`). |
| `125..=94` | `epoch` | `32` bits (`1..=u32::MAX`) | Encodes `intent_exec_epoch` so cold-restarted gateways advance per-strategy epoch floors past live open orders. |
| `93..=30` | `intent_id` | `64` bits (`1..=2^63 - 1`) | Encodes `intent_id` / `order_seqnum` so cold-restarted workers and gateways never reuse an active sequence number. |
| `29..=0` | `tag` | `30` bits | Truncated `SHA-256("tickwire.client-id.tw1" \|\| risk_domain_id \|\| venue \|\| env \|\| account \|\| product \|\| strategy_id \|\| leg \|\| epoch \|\| intent_id)` rejecting manual/external or cross-domain client IDs ($2^{-30} \approx 9.3 \times 10^{-10}$ collision bound). |

For every verified `SelfDescribingClientId`, `deterministic_order_id(&ClientIdDomain)` derives a nonzero 16-byte internal `order_id` via `SHA-256("tickwire.client-order-uuid.v1" || domain_fields || strategy_id || leg || epoch || intent_id)[..16]` (setting bit `0` of byte `0` if all 16 bytes are zero). Client IDs that do not carry the `"tw1"` prefix or fail `tag` verification for the bound `ClientIdDomain` classify cleanly as `External` (manual or foreign orders) rather than corrupting attribution.

### Observation Provenance and Fill Economics

Every `VenueEvent` records `Scope`, `Product`, `instrument_version`, source `connection_epoch`, optional source sequence, native event ID, and timestamps. Reconnecting to a venue advances `connection_epoch` to record new session provenance, while the native venue trade ID continues to uniquely identify each economic fill. Uncorrelated venue events encode zero internal order and intent IDs and must carry a native venue order, list, or algo ID for downstream association.

Fill payloads strictly separate order-level price and cumulative quantities from the individual execution's price, quantity, liquidity role, and native trade ID. Optional cumulative quote, `last_quote`, and `AssetDelta` realized PnL remain distinct (`None` when omitted by the venue). A single event may carry signed fees across up to **8 distinct assets**, each naming its asset ID, version, symbol, and exponent; duplicate asset identities within the same fee list fail validation, requiring adapters either to aggregate same-asset components or emit a dedicated fee event.

### Native Modify Events and Unified ORS `TerminalReason` Classification

To support single-round-trip in-place price/quantity modifications (`CommandBody::Modify`, command tag `5`) as well as unsolicited exchange price-band/quantity clamps across Spot, USD-M, COIN-M, and Options venues, `VenueEventBody` includes two schema-`5` variants alongside tags `1..=7`:

* **`VenueEventBody::Modified(Box<ModifyObservation>)` (tag `8`)**: Carries `target: OrderTarget`, optional `amendment_id: Option<VenueToken>`, optional `intent: Option<IntentKey>`, `prior_client_id` and `new_client_id`, optional `venue_order_id`, venue-applied `new_quantity: Decimal` (`> 0`) and `new_price: Decimal` (`> 0`), and optional non-negative `cumulative_filled: Option<Decimal>` (`<= new_quantity`).
* **`VenueEventBody::ModifyRejected(Box<ModifyRejection>)` (tag `9`)**: Carries `target: OrderTarget`, optional `amendment_id`, optional `intent`, optional `new_client_id`, optional `venue_order_id`, optional numeric venue error `code: Option<i64>`, and `reason: TerminalReason` encoded as a compact `u8` CBOR discriminant (`0..=30`, rejecting unknown discriminants with `Error::Enum`).

`TerminalReason` unifies venue execution outcomes and stateless ORS gateway admission/routing refusals across compact `u16` wire codes (`0..=30`) and associated ORS constant aliases:

* **Exchange lifecycle & execution reasons**: `Filled` (`0`), `UserCanceled` (`1`), `CrossesBook` (`2`), `SelfTradePreventedByVenue` (`3`, `MKT_CANCELED_SELFTRADE`), `PostOnlyWouldTake` (`4`, `MKT_REJECT_POST_ONLY`), `ExpiredIocOrFok` (`5`, `MKT_IOC_CANCELED`), `ReduceOnlyClippedOrCanceled` (`6`, `MKT_REDUCE_ONLY_CANCELED`), `InsufficientFunds` (`7`), `RateLimited` (`8`), `OrderNotOpen` (`9`, `MKT_REJECT_NO_SUCH_ORDER`), `TooManyPendingActions` (`10`), `InvalidPrice` (`11`), `InvalidQuantity` (`12`), `NoChange` (`13`), `AbortedBeforeSend` (`14`), `VenueRejected` (`15`, `MKT_REJECT`), `Unknown` (`16`), `NoReason` (`17`, `NO_REASON` — strategy-initiated cancel confirmed when `pending_cxl == true`), `MarketCanceled` (`18`, `MKT_CANCELED` — unsolicited venue cancel when `pending_cxl == false`), and `MarketDelayed` (`19`, `MKT_DELAYED` — ambiguous send/execution status such as `-1007` / `-1000` triggering status reconciliation).
* **ORS gateway & session refusal reasons**: `RejectStaleOrder` (`20`, `REJECT_STALE_ORDER` — `now_mono_ns - tick_mono_ns > max_order_age_ns`), `AllocationError` (`21`, `ALLOCATION_ERROR` — order-slot CAS collision), `UnexpectedMsgSeqnum` (`22`, `UNEXPECTED_MSG_SEQNUM` — non-monotonic `msg_seqnum` / `order_seqnum`), `InvalidStratId` (`23`, `INVALID_STRAT_ID` — `strat_id == 255` on login/order, unconfigured strategy, or duplicate login), `InvalidAccountId` (`24`, `INVALID_ACCOUNT_ID`), `RejectRoutingIdMismatch` (`25`, `REJECT_ROUTING_ID_MISMATCH` — strategy not authorized for `account_ref`), `AdminAction` (`26`, `ADMIN_ACTION` — operator/logout/supervisor cancel), `RestrictedCancelOnly` (`27`, `RESTRICTED_CANCEL_ONLY` — ORS suspended into cancel-only mode), `MarketRejectMarketDown` (`28`, `MKT_REJECT_MARKET_DOWN` — no established venue session or market closed), `RfqQuoteNotSupported` (`29`, `RFQ_QUOTE_NOT_SUPPORTED`), and `InternalCancel` (`30`, `INTERNAL_CANCEL` — status query exhausted `max_query_count`).

Zero-allocation classifiers (`classify_binance_reason`, `classify_spot_venue_reason`, and `classify_ors_cancel_reason(..., pending_strategy_cancel)`) map venue error codes, STP modes, Post-Only/RPI maker refusals, IOC/FOK expiries, and pending-cancel state (`NO_REASON` vs `MKT_CANCELED`) without heap allocation.

## Core Invariants

* **Canonical CBOR determinism**: Integers, array/string lengths, and CBOR bignum tags `2` and `3` use shortest-form encodings across the full `i128` range (with negatives encoded as `-1 - magnitude`, including `i128::MIN`). Decimals encode as `[coefficient, exponent]` normalized over exponents `-38..=38`, with zero canonically `[0, 0]`. Floating-point values and indefinite-length items are forbidden. Structs serialize in declared field order, `Option<T>` encodes as `[]` or `[value]`, and enum tags plus variant arities are explicit.
* **Overflow-free tick and step alignment**: Validation verifies required nonzero IDs, deadlines, trigger references, product/contract combinations (`Spot`, `UsdM`, `CoinM`, `Options`, `LinearPerp`, `InversePerp`, `LinearFutures`, `InverseFutures`), order-type/TIF/price rules (including `TimeInForce::Rpi`), close-position constraints, and tick/step divisibility—computing decimal alignment via unsigned remainders so large differing exponents cannot overflow `i128` multiplication.
* **Strict event-version pairing**: `CanonicalEvent::decode` requires the envelope version (`1..=5`) to match the exact version mandated by `VenueEventBody`, rejecting mismatched versions with `Error::Schema`.
* **Immutable client identity across transport retries**: Transport timestamps, signatures, and protocol request IDs generated by an adapter never rewrite `ClientId`, `CanonicalCommand::digest()`, or the business `deadline`.

## State Transitions and Failure Semantics

Decoding or constructing a command, event, `SelfDescribingClientId`, or `Fill` fails atomically with a typed `tickwire_venue::Error` (`Bounds`, `Text`, `Decimal`, `IntegerOverflow`, `Truncated`, `NonCanonical`, `Schema`, `Field`, `Enum`, `TrailingData`, `Identity`, `Instrument`, `Deadline`, `Quantity`, `Price`, `OrderCombination`, `ProductCombination`, or `Duplicate`) whenever any syntactic or semantic rule is violated. Passing `tickwire-venue` validation proves well-formedness and byte identity; the `Catalog`, `Account`, ORS session manager, and venue adapter still verify live venue support, price bands, fee schedules, minimum notional, account margin modes, strategy routing authorization, and claimable position reductions before an order is admitted or transmitted.

## Verification and References

```sh
cargo test --locked -p tickwire-venue --all-targets
```

* Implementation modules: [canonical wrappers and schema versions](../../crates/tickwire-venue/src/lib.rs), [command, routing, and ORS report types](../../crates/tickwire-venue/src/types.rs), [terminal and ORS refusal classifiers](../../crates/tickwire-venue/src/reasons.rs), [self-describing `tw1` client IDs](../../crates/tickwire-venue/src/client_id.rs), [canonical account inputs](../../crates/tickwire-venue/src/account.rs), [synchronous request replies](../../crates/tickwire-venue/src/request_reply.rs), [CBOR reader/writer](../../crates/tickwire-venue/src/cbor.rs), [schema codec](../../crates/tickwire-venue/src/codec.rs), [semantic validator](../../crates/tickwire-venue/src/validate.rs)
* Test suites: [contract and routing tests](../../crates/tickwire-venue/tests/contract.rs), [golden vector tests](../../crates/tickwire-venue/tests/vectors.rs), [`tw1` client-ID tests](../../crates/tickwire-venue/tests/client_id.rs), [modify and ORS reason tests](../../crates/tickwire-venue/tests/modify_and_reasons.rs), [strict CBOR decoding tests](../../crates/tickwire-venue/tests/strict.rs), [event validation tests](../../crates/tickwire-venue/tests/events.rs)
* Related specifications: [IPC wire codec](ipc-codec.md), [conditional orders](conditional-orders.md), [spot keep-priority amend](spot-amend.md)
