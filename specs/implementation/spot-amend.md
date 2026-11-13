# Spot Keep-Priority Quantity Reduction

`AmendKeepPriority` lowers the effective total quantity of an existing plain Spot `LIMIT`/`GTC` order in place without canceling and replacing its book priority. Because an in-flight amendment races with exchange fills on the pre-amendment quantity, `tickwire-account` treats admission of `AmendKeepPriority` as a pure management intent: it allocates no second order slot and releases zero reserved funds until authenticated venue confirmation and trade evidence establish the post-amendment outcome.

Separating amendment admission from principal release prevents under-reservation when an order fills partially—or completely up to its original quantity—before the venue processes the reduction. Only Binance Spot implements `AmendKeepPriority` across REST, amendment-history queries, and private execution reports; the Bybit and OKX cash adapter documents its narrower command surface separately in [spot-execution.md](spot-execution.md).

## State Model and Bounds

| Aspect | Rule or Wire Representation | Enforcement Layer |
| --- | --- | --- |
| Eligible target order | Plain Spot `LIMIT`/`GTC` order only (iceberg orders and conditional structure legs are rejected). | `tickwire-venue` & `tickwire-account/src/amendments.rs` |
| Identity & ownership match | Exact `ClientId`, original intent execution epoch, internal and native order IDs, current `ManagerIdentity`, and `owner_revision`. | Checked at `Account::admit` and rechecked alongside known `filled` quantity before the first TLS byte. |
| Quantity inequality | `booked_filled < proposed_total < current_effective_total`; at most **1** pending amendment per order. | `Account::admit` |
| Event schema | `CanonicalEvent` **schema 3**, body tags **`6`** (`Amendment`) and **`7`** (`AmendmentRejected`); schemas `1` and `2` keep their exact byte meanings. | `tickwire-venue/src/codec.rs` |
| History capacity | Bounded per-account amendment table; capacity exhaustion rejects new entries rather than evicting historical IDs needed for replay deduplication. | `tickwire-account/src/amendments.rs` |
| State-digest domain | Outer digest **v4** embedding bounded **`amendment-state.v1`** preimage data for orders with amendment history. | Durable observation anomalies and liabilities affect the digest; recovery-injected `recovery_unknown` and `frozen` flags do not. |

## Core Invariants

* **Zero principal release on admission or HTTP encoding**: Admitting `AmendKeepPriority`, encoding the protocol request, or merely sending bytes never lowers reserved cash or base inventory. Only a matched venue `Amendment` observation updates `OrderView.effective_quantity`, while `OrderView.original_quantity` remains immutable.
* **Dedicated `amend` grant with narrow market-readiness bypass**: An amendment requires the strategy's `StrategyGrant` with `Actions.amend == true`, an unexpired `OwnerLease`, and matching `writer_epoch`, `config_revision`, and `deadline`; an administrative `ManagementGrant` (`cancel`/`query`) can never authorize `AmendKeepPriority`. In `ControlPlane`, skipping public market-liveness and private-reconciliation checks applies *exclusively* when `CommandBody::AmendKeepPriority` carries a closed `Admission` with `!admission.increases_risk()` (`Modify` and `CancelReplace` remain subject to full `Mode::Trading` qualification).
* **Monotonic effective-quantity ceiling**: Remaining order liability equals the non-negative part of `max(effective_total, observed_post_amend_cumulative) - booked_filled`. Once `effective_quantity` is reduced by a confirmed amendment, a delayed status report still carrying the larger pre-amendment quantity cannot raise `effective_quantity` again.
* **Cumulative anomalies freeze without fabricating fills**: Observing a post-amendment cumulative fill quantity without every supporting individual trade freezes `Account` and retains the unexplained liability gap rather than synthesizing a fake fill. Conversely, real trades arriving beyond the reduced `effective_total` still book actual cash and inventory deltas and immediately freeze `Account`.

## State Transitions and Failure Semantics

### Multi-Source Correlation and Replay

Binance reports amendment outcomes across three distinct channels—the synchronous REST response, read-only amendment history pagination, and private WebSocket execution reports—each carrying channel-specific execution identifiers. `AmendmentView` records the original request, prior and new quantities, and per-source identifiers so duplicate reports across channels recognize already-applied amendments while still forwarding any newly attached order-status fields (and refusing to rebind onto a different pending amendment). During crash recovery, verifiers re-parse raw persisted input bytes and validate each normalized event against the bound `CanonicalCommand`.

### Abort Contradictions

If transport proves zero bytes were sent (`Aborted`) before any confirmation arrives, the pending amendment terminates cleanly and leaves the target Spot order live at its prior `effective_quantity`. If an authentic venue `Amendment` confirmation subsequently arrives contradicting that local abort, `Account` applies and retains the amendment observation and freezes new risk. Conversely, a late zero-byte transport report arriving *after* a confirmed amendment is rejected and cannot revert the confirmed `effective_quantity`.

## Verification and References

```sh
cargo test --locked -p tickwire-account --test amend
cargo test --locked -p tickwire-runtime-policy -p tickwire-binance-account
```

* Core reducer, policy, and wire codec: [account amendment reducer and v4 digests](../../crates/tickwire-account/src/amendments.rs), [runtime policy gate](../../crates/tickwire-runtime-policy/src/lib.rs), [schema-3 event codec](../../crates/tickwire-venue/src/codec.rs)
* Binance REST, history, and private stream adapters: [REST amendment response handler](../../crates/tickwire-binance-account/src/rest/amendment.rs), [amendment history reader](../../crates/tickwire-binance-account/src/reconciliation/amendments.rs), [private amendment normalizer](../../crates/tickwire-binance-account/src/amendment.rs)
* Test suites and related specs: [account amendment tests](../../crates/tickwire-account/tests/amend.rs), [multi-asset account state machine](account.md), [order submission policy](runtime-policy.md), [Bybit and OKX spot execution](spot-execution.md)
