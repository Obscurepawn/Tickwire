# Canonical Trading Commands and Venue Events

`tickwire-venue` defines the immutable wire representations, SHA-256 content identities, and structural validators for trading commands (`CanonicalCommand`) and normalized exchange observations (`CanonicalEvent`, `CanonicalAccountInput`). Strategy workers, recovery stores, write-ahead journals, and execution gateways share this crate so every subsystem hashes and interprets the exact same canonical CBOR byte sequence. `tickwire-venue` forbids `unsafe` code and owns deterministic serialization and parameter validation; venue protocol signing belongs to execution adapters, and balance, margin, and authorization enforcement belong to `Account` and `AccountGateway`.

Separating mutable draft construction (`Command`, `VenueEvent`) from sealed canonical values (`CanonicalCommand`, `CanonicalEvent`) guarantees that neither an unvalidated struct nor a non-canonical CBOR payload can obtain a `Hash` digest or cross process and journal boundaries.

## Encoding and Identity Model

`Command` is an editable draft, whereas `CanonicalCommand` privately owns the validated `Command`, its canonical CBOR byte slice, and its SHA-256 `digest`, exposing read-only accessors. Both `CanonicalCommand::new` and `CanonicalCommand::decode` (and the analogous `CanonicalEvent` constructors) run full semantic validation and verify that re-encoding produces the exact input bytes.

```text
command = ["tickwire.venue-command", 1, CommandContext, CommandBody]
event   = ["tickwire.venue-event", version, EventIdentity, VenueEventBody]
digest  = SHA256(complete top-level encoding)
```

| Envelope / Component | Bound or Versioning Rule | Semantic Scope |
| --- | --- | --- |
| Payload size | `MAX_CANONICAL_BYTES = 16,384` (16 KiB) | Enforced prior to allocation and after serialization; string and array lengths check capacity before allocating. |
| `CommandContext` | Schema version `1` | Binds `risk_domain_id`, `venue`, `environment`, `account`, `product`, `intent_id`, `intent_exec_epoch`, `strategy_generation`, `config_revision`, `deadline`, and `recovery_object`, plus strategy lineage (`logical_strategy_id`, `instance_id`, `intent_seq`, `authorization_epoch`, policy/config versions, `created_mono_ns`, economic intent, hedge group/leg, existing manifest hash) and market trigger references. |
| `InstrumentBinding` | Exact UTF-8 `native_symbol` | Carries instrument/catalog IDs, versions, and digest, base/quote/settlement `AssetRef`s, price tick and quantity step increments, contract kind, and optional delivery/expiry timestamp. Rejects whitespace and control characters; preserves case and Unicode form. |
| `VenueEvent` version `1` | Classic order, fill, and fee lifecycle | Covers standard order status transitions, fee events, and fills carrying a reported lifecycle (including legacy fills with `OrderStatus::Unknown` tag `8`). |
| `VenueEvent` version `2` | Conditional / list structures | Encodes `VenueEventBody::Structure` observations (body tag `5`). |
| `VenueEvent` version `3` | Spot keep-priority amendments | Encodes `Amendment` confirmations and `AmendmentRejected` observations (tags `6` and `7`). |
| `VenueEvent` version `4` | Lifecycle-free trade rows | Encodes Bybit and OKX single-trade fills using `OrderStatus::Unreported` (status tag `9`), booking cash and fees without closing the order. |
| `VenueEvent` version `5` | Native order modifications | Encodes `VenueEventBody::Modified` (`ModifyObservation`, body tag `8`) and `VenueEventBody::ModifyRejected` (`ModifyRejection`, body tag `9`) across both Spot and derivative products, preserving backward compatibility for tags `1..=7`. |

### Command Bodies and Multi-Leg Structures

Single-order specs capture `ClientId`, `Side`, `OrderType`, quantity unit (including quote-asset quantities on ordinary spot market buys and sells), limit price, `TimeInForce`, `PositionSide`, trading/margin modes, `reduce_only`, self-trade prevention, trigger references, trailing terms (basis points for spot; percentage units plus explicit working-price source for derivatives), and iceberg quantities. Order-management variants (`Cancel`, `CancelList`, `CancelAlgo`, `Query`, `AmendKeepPriority`, `Modify`, `CancelReplace`) preserve the target's original identity, management/owner revision, and execution epoch while encoding replacement parameters explicitly.

Multi-leg conditional commands carry a dedicated parent identity and enforce deterministic leg ordering:

* **Spot OCO**: `[above, below]`
* **Spot OTO**: `[working, pending]`
* **Spot OTOCO**: `[working, pendingAbove, pendingBelow]`

The `CommandBody` enum also models supported spot extensions explicitly; a command can be canonically valid in `tickwire-venue` even when a particular gateway profile declines to execute it.

### Self-Describing Client Order Identity (`SelfDescribingClientId`)

To allow cold stateless Pod restarts and exchange-only recovery to reconstruct strategy attribution, execution epoch, intent sequence floors, and internal order UUIDs directly from open orders returned by venue REST endpoints—without relying on local origin files or surviving disk state—`tickwire-venue::client_id` defines a fixed-width, domain-authenticated `ClientId` encoding (`SelfDescribingClientId`):

```text
wire = "t1" || base32_lower_30(150_bit_payload)   // 32 ASCII [0-9a-z] characters
```

The 30-character payload uses the lowercase Crockford-style alphanumeric alphabet `0123456789abcdefghjkmnpqrstvwxyz` (5 bits per character, strictly `[0-9a-z]`), satisfying OKX `clOrdId` (`1..=32` ASCII alphanumeric), Bybit `orderLinkId` (`1..=36`), and Binance Spot/USDⓈ-M `newClientOrderId` (`1..=36`) simultaneously:

| Bit Slice (MSB → LSB) | Field | Width / Domain | Purpose |
| --- | --- | --- | --- |
| `149..=134` | `logical_strategy_id` | `16` bits (`1..=65,535`) | Recovers originating strategy ownership across `Account::MAX_STRATEGIES` (`1,024`) with 64× headroom. |
| `133..=130` | `leg` | `4` bits (`0..=15`) | Disambiguates single orders (`0`), cancel-replace legs (`1`), multi-leg `SpotList` children (`0..=2`), and parent list/algo IDs (`15`). |
| `129..=98` | `epoch` | `32` bits (`1..=u32::MAX`) | Encodes `intent_exec_epoch` so cold-restarted gateways advance per-strategy epoch floors past live open orders. |
| `97..=34` | `intent_id` | `64` bits (`1..=2^63 - 1`) | Encodes `intent_id` so cold-restarted workers and gateways never reuse an active sequence number. |
| `33..=0` | `tag` | `34` bits | Truncated `SHA-256("tickwire.client-id.v1" \|\| risk_domain_id \|\| venue \|\| env \|\| account \|\| product \|\| strategy_id \|\| leg \|\| epoch \|\| intent_id)` rejecting manual/external or cross-domain client IDs ($2^{-34} \approx 5.8 \times 10^{-11}$ collision bound). |

For every verified `SelfDescribingClientId`, `deterministic_order_id(&ClientIdDomain)` derives a nonzero 16-byte internal `order_id` via `SHA-256("tickwire.client-order-uuid.v1" || domain_fields || strategy_id || leg || epoch || intent_id)[..16]` (setting bit `0` of byte `0` if all 16 bytes are zero), eliminating local `Origins` alias files on cold recovery. Client IDs that do not carry the `"t1"` prefix or fail `tag` verification for the bound `ClientIdDomain` classify cleanly as `External` (manual or foreign orders) rather than corrupting attribution.

### Observation Provenance and Fill Economics

Every `VenueEvent` records `Scope`, `Product`, `instrument_version`, source `connection_epoch`, optional source sequence, native event ID, and timestamps. Reconnecting to a venue advances `connection_epoch` to record new session provenance, while the native venue trade ID continues to uniquely identify each economic fill. Uncorrelated venue events encode zero internal order and intent IDs and must carry a native venue order, list, or algo ID for downstream association.

Fill payloads strictly separate order-level price and cumulative quantities from the individual execution's price, quantity, liquidity role, and native trade ID. Optional cumulative quote, `last_quote`, and `AssetDelta` realized PnL remain distinct (`None` when omitted by the venue). A single event may carry signed fees across up to **8 distinct assets**, each naming its asset ID, version, symbol, and exponent; duplicate asset identities within the same fee list fail validation, requiring adapters either to aggregate same-asset components or emit a dedicated fee event.

### Native Modify Events and Fine-Grained `TerminalReason` Classification

To support single-round-trip in-place price/quantity modifications (`CommandBody::Modify`, command tag `5`) as well as unsolicited exchange price-band/quantity clamps across both Spot and derivative venues, `VenueEventBody` adds two schema-`5` variants without altering tags `1..=7`:

* **`VenueEventBody::Modified(Box<ModifyObservation>)` (tag `8`)**: Carries `target: OrderTarget`, optional `amendment_id: Option<VenueToken>`, optional `intent: Option<IntentKey>`, `prior_client_id` and `new_client_id`, optional `venue_order_id` (supporting native ID rotation on cancel-replace modify endpoints), venue-applied `new_quantity: Decimal` (`> 0`) and `new_price: Decimal` (`> 0`, which may differ from the requested values when clamped by a venue price band), and optional non-negative `cumulative_filled: Option<Decimal>` (`<= new_quantity`).
* **`VenueEventBody::ModifyRejected(Box<ModifyRejection>)` (tag `9`)**: Carries `target: OrderTarget`, optional `amendment_id`, optional `intent`, optional `new_client_id`, optional `venue_order_id`, optional numeric venue error `code: Option<i64>`, and `reason: TerminalReason` encoded as a compact `u8` CBOR discriminant (`0..=16`, rejecting unknown discriminants with `Error::Enum`).

`TerminalReason` (`Filled`, `UserCanceled`, `CrossesBook`, `SelfTradePreventedByVenue`, `PostOnlyWouldTake`, `ExpiredIocOrFok`, `ReduceOnlyClippedOrCanceled`, `InsufficientFunds`, `RateLimited`, `OrderNotOpen`, `TooManyPendingActions`, `InvalidPrice`, `InvalidQuantity`, `NoChange`, `AbortedBeforeSend`, `VenueRejected`, `Unknown`) provides `as_str()`, `is_benign_business_refusal()`, and zero-allocation classifiers (`classify_binance_reason` and `classify_spot_venue_reason`) mapping Binance, Bybit, and OKX error codes, STP cancel modes, GTX/Post-Only refusals, and IOC/FOK expiries without heap allocation.

## Core Invariants

* **Canonical CBOR determinism**: Integers, array/string lengths, and CBOR bignum tags `2` and `3` use shortest-form encodings across the full `i128` range (with negatives encoded as `-1 - magnitude`, including `i128::MIN`). Decimals encode as `[coefficient, exponent]` normalized over exponents `-38..=38`, with zero canonically `[0, 0]`. Floating-point values and indefinite-length items are forbidden. Structs serialize in declared field order, `Option<T>` encodes as `[]` or `[value]`, and enum tags plus variant arities are explicit.
* **Overflow-free tick and step alignment**: Validation verifies required nonzero IDs, deadlines, trigger references, product/contract combinations, order-type/TIF/price rules, close-position constraints, and tick/step divisibility—computing decimal alignment via unsigned remainders so large differing exponents cannot overflow `i128` multiplication.
* **Strict event-version pairing and rollout safety**: `CanonicalEvent::decode` requires the envelope version to match the exact version mandated by `VenueEventBody`. Because version-`1..=3` readers reject version `4` (`Error::Schema`), operators must upgrade every account-log reader and recovery verifier before enabling a version-`4` writer on a shared journal (outer physical WAL framing and strategy IPC status fields keep their existing schemas).
* **Immutable client identity across transport retries**: Transport timestamps, signatures, and protocol request IDs generated by an adapter never rewrite `ClientId`, `CanonicalCommand::digest()`, or the business `deadline`.

## State Transitions and Failure Semantics

Decoding or constructing a command or event fails atomically with a typed `tickwire_venue::Error` (`Bounds`, `Text`, `Decimal`, `IntegerOverflow`, `Truncated`, `NonCanonical`, `Schema`, `Field`, `Enum`, `TrailingData`, `Identity`, `Instrument`, `Deadline`, `Quantity`, `Price`, `OrderCombination`, `ProductCombination`, or `Duplicate`) whenever any syntactic or semantic rule is violated. Passing `tickwire-venue` validation proves well-formedness and byte identity; the trusted `Catalog`, `Account`, `ControlPlane`, and venue adapter must still verify live venue support, price bands, fee schedules, minimum notional, account margin modes, management permissions, and claimable position reductions before an order is admitted or transmitted.

## Verification and References

```sh
cargo test --locked -p tickwire-venue --all-targets
```

* Implementation modules: [canonical wrappers and schema versions](../../crates/tickwire-venue/src/lib.rs), [command and event types](../../crates/tickwire-venue/src/types.rs), [canonical account inputs](../../crates/tickwire-venue/src/account.rs), [CBOR reader/writer](../../crates/tickwire-venue/src/cbor.rs), [schema codec](../../crates/tickwire-venue/src/codec.rs), [semantic validator](../../crates/tickwire-venue/src/validate.rs)
* Test suites and formal models: [golden vector tests](../../crates/tickwire-venue/tests/vectors.rs), [strict CBOR decoding tests](../../crates/tickwire-venue/tests/strict.rs), [event validation tests](../../crates/tickwire-venue/tests/events.rs), [L1 command-binding TLA+ input](../../tla/execution_models/business_journal/L1_command_binding/00_input.md)
* Related specifications: [binding a persisted intent to its command](command-binding.md), [conditional orders](conditional-orders.md), [spot keep-priority amend](spot-amend.md)
