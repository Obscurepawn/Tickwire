# Spot Keep-Priority Quantity Reduction

`AmendKeepPriority` lowers the effective total quantity of an existing plain Spot `LIMIT`/`GTC` order in place without canceling and replacing its book priority. Because an in-flight amendment races with exchange fills on the pre-amendment quantity, `tickwire-account` treats admission of `AmendKeepPriority` as a pure management intent: it allocates no second order slot and releases zero reserved funds until authenticated venue confirmation and trade evidence establish the post-amendment outcome.

Separating amendment admission from principal release prevents under-reservation when an order fills partially—or completely up to its original quantity—before the venue processes the reduction.

## State Model and Bounds

| Aspect | Rule or Wire Representation | Enforcement Layer |
| --- | --- | --- |
| Eligible target order | Plain Spot `LIMIT`/`GTC` order only (iceberg orders and conditional structure legs are rejected). | `tickwire-venue` & `tickwire-account/src/amendments.rs` |
| Identity & ownership match | Exact `ClientId`, original intent execution epoch, internal and native order IDs, current `ManagerIdentity`, and `owner_revision`. | Checked at `Account::admit` and `Account::check_admission`. |
| Quantity inequality | `booked_filled < proposed_total < current_effective_total`; at most **1** pending amendment or `PendingModify` per order (`Error::TooManyPendingActions` on concurrent in-flight actions; full price or quantity-increase updates use `CommandBody::Modify` / schema-`5` tags `8` and `9` documented in [account.md](account.md)). | `Account::admit` |
| Event schema | `CanonicalEvent` **schema 3**, body tags **`6`** (`Amendment`) and **`7`** (`AmendmentRejected`); schemas `1` and `2` keep their exact byte meanings. | `tickwire-venue/src/codec.rs` |
| History capacity | Bounded per-account amendment table; capacity exhaustion rejects new entries rather than evicting active deduplication state. | `tickwire-account/src/amendments.rs` |
| State-digest domain | Outer digest **v4** embedding bounded **`amendment-state.v1`** preimage data for orders with amendment history. | Observation anomalies and liabilities affect the digest; volatile `recovery_unknown` and `frozen` flags do not. |

## Core Invariants

* **Zero principal release on admission**: Admitting `AmendKeepPriority` never lowers reserved cash or base inventory. Only a matched venue `Amendment` observation updates `OrderView.effective_quantity`, while `OrderView.original_quantity` remains immutable.
* **Risk-non-increasing management classification**: `AmendKeepPriority` carries `!admission.increases_risk()` and requires exact strategy ownership (`ManagerIdentity` and `owner_revision`) rather than generic cancel-only operator overrides, while full price or quantity-increase modifications (`Modify` / `CancelReplace`) remain risk-increasing admissions.
* **Monotonic effective-quantity ceiling**: Remaining order liability equals the non-negative part of `max(effective_total, observed_post_amend_cumulative) - booked_filled`. Once `effective_quantity` is reduced by a confirmed amendment, a delayed status report still carrying the larger pre-amendment quantity cannot raise `effective_quantity` again.
* **Cumulative anomalies freeze without fabricating fills**: Observing a post-amendment cumulative fill quantity without every supporting individual trade freezes `Account` and retains the unexplained liability gap rather than synthesizing a fake fill. Conversely, real trades arriving beyond the reduced `effective_total` still book actual cash and inventory deltas and immediately freeze `Account`.

## State Transitions and Failure Semantics

### Multi-Source Correlation

Venues report amendment outcomes across synchronous order replies, read-only amendment queries, and private WebSocket execution reports—each carrying channel-specific execution identifiers. `AmendmentView` records the original request, prior and new quantities, and per-source `(AmendmentSource, execution_id)` pairs so duplicate reports across channels recognize already-applied amendments while still forwarding any newly attached order-status fields (and refusing to rebind onto a different pending amendment).

### Abort Contradictions

If transport proves zero bytes were sent (`Aborted`) before any confirmation arrives, the pending amendment terminates cleanly and leaves the target Spot order live at its prior `effective_quantity`. If an authentic venue `Amendment` confirmation subsequently arrives contradicting that local abort, `Account` applies and retains the amendment observation and freezes new risk. Conversely, a late zero-byte transport report arriving *after* a confirmed amendment is rejected and cannot revert the confirmed `effective_quantity`.

## Verification and References

```sh
cargo test --locked -p tickwire-account --test amend
```

* Core reducer and wire codec: [amendments.rs](../../crates/tickwire-account/src/amendments.rs), [codec.rs](../../crates/tickwire-venue/src/codec.rs)
* Test suites and related specs: [amend.rs](../../crates/tickwire-account/tests/amend.rs), [Stateless account and OrderManager](account.md), [Venue domain types](venue.md)
