# Conditional Orders, Parent Identity, and Leg Liability

Multi-leg Spot order lists (`SpotList`: OCO, OTO, OTOCO) and USDⓈ-M conditional algorithms (`Algo`) separate parent coordination state from child order execution on the exchange. `tickwire-account` mirrors that separation directly: admitting a conditional command creates one immutable parent `Structure` alongside up to three potential leg `Order` entries, where each leg owns its own principal or reduction liability and the parent reserves nothing a second time. When a leg triggers on the venue, the resulting native child order binds onto its pre-admitted leg rather than allocating unbudgeted exposure.

Keeping parent lifecycle observations distinct from leg execution facts prevents premature reserve release. On both Binance Spot and USDⓈ-M, a parent list or algo order can report a terminal status while a triggered child order still has unbooked fills or open fee liability; only per-child order queries and complete trade-history pagination—or a cryptographically verified proof that a USDⓈ-M algo never spawned a child—can close leg liabilities.

## State Model and Encoding Bounds

| Layer / Artifact | Schema, Tag, or Bound | Structural Contract |
| --- | --- | --- |
| Conditional admission | `1` parent (`[u8; 16]`), `1..=3` legs (`MAX_ADMISSION_ORDERS = 3`) | Fixes parent `StructureKind`, `command_digest`, and ordered leg `OrderKey`s (`[above, below]` for OCO, `[working, pending]` for OTO, `[working, pendingAbove, pendingBelow]` for OTOCO). |
| Structure observation | `CanonicalEvent` schema `2`, body tag `5` | Validates `Product`, original intent and execution epoch, parent identity, member count cap, and child uniqueness. |
| Parent REST query input | Input WAL tag `21` | Persists read-only Spot `/api/v3/orderList` and USDⓈ-M `/fapi/v1/algoOrder` requests, clock domain, and raw HTTP responses under the structure-query quota. |
| `UncreatedAlgoClosed` | Account record tag `16` (`13` fields); `25`-field closure ≤ 4,096 bytes | Binds `scope`, `account_epoch`, `ledger_sequence`, `baseline_digest`, `history_digest`, `input_stream`, `through_sequence`, state/coverage digests, `structure_id`, original `intent`, `command_digest`, and native parent ID to 1 parent query and 3 snapshots (by sequence, byte length, and SHA-256). |
| State-digest versions | Outer `v1` → `v2` (`conditional-state.v1`) → `v3` (`conditional-state.v2`) | Stays at `v1` until the first structure observation (`v2`), and advances to `v3` upon the first durable tag-`16` closure. Parents sort by `structure_id`, legs keep command order, children sort by `ClientId`, integers encode little-endian, and text is length-prefixed. |

## Core Invariants

* **Terminal parents never close child liabilities**: Parent acknowledgement, trigger, terminal, and unknown transitions persist as independent structure facts (with exact duplicates leaving `ledger_sequence` unchanged), but a terminal parent response, Spot member list, or USDⓈ-M aggregate `actualQty` never synthesizes a child trade, closes child fees, or releases leg reservations. Every *created* child requires its own native order query and contiguous trade-history pages from the initial cursor whose sum matches reported cumulative quantity.
* **All-leg ownership checks on cancellation**: `CancelList` and `CancelAlgo` verify scope, instrument, original parent identity, and *every* leg's current `ManagerIdentity` and `owner_revision` at admission and again before the first TLS output byte. Replaying a historical `StrategyHandover` installs the recorded manager and revision across affected legs.
* **Contradicted aborts restore bounded leg liability**: A parent acknowledgement forbids any subsequent local zero-byte abort. If an acknowledgement, fill, or fee arrives *after* a local abort, `Account` restores leg principal and derivative reduction reservations from the original durable `OrderSpec` (`original_quantity - booked_filled`, incorporating the new fill where applicable—never enlarging reduction size or switching side or position mode) and freezes the account pending reconciliation. Conflicting native parent or child IDs retain the original binding and freeze the account.
* **Permanent audit trail in conditional state digests**: Advancing outer account-input and strategy-order digests to `v3` (`tickwire.conditional-state.v2`) permanently hashes uncreated-leg closure state bits and closure digests into the state preimage, even if a late exchange child observation later revokes the uncreated flag and restores leg liability.

## State Transitions and Failure Semantics

### Binance Provenance and Child Correlation

The Binance input owner binds private WebSocket parent reports and REST/WS command responses to the registered `CanonicalCommand` and active session, synchronizing raw evidence to the input WAL before applying `Account` transitions (and replaying from the same `InputKey` to repair any interrupted account-WAL append):

* **Spot order lists**: Member lists establish parent-to-leg membership, while per-leg lifecycle and fill status come exclusively from individual order and execution reports.
* **USDⓈ-M algo orders**: Every native child order must bind through a known parent. An unidentified child arriving ahead of parent registration is quarantined, previously bound native IDs can never be reassigned to another leg, and a parent report carrying a missing `actualOrderId` alongside a nonzero trigger timestamp/price or non-empty `actualType` marks the structure outcome unknown.

### Proving a Potential USDⓈ-M Algo Leg Was Never Created

When a USDⓈ-M conditional parent terminates before triggering, no native child order exists on `/fapi/v1/order`. Closing the pre-admitted potential leg's reservation requires dedicated gateway verification via `AccountGateway::close_uncreated_algo` (generic fact append paths reject tag `16`):

1. **Qualifying parent query**: The persisted `/fapi/v1/algoOrder` response (input tag `21`) must report status `CANCELED`, `EXPIRED`, or `REJECTED`, an explicitly empty `actualOrderId`, absent or empty `actualType`, absent or zero `actualQty`, and present zero `actualPrice` and `triggerTime`, with zero conflicting child, trigger, or fill observations anywhere in retained history.
2. **Cut and three-snapshot ordering**: The verified cut binds physical writer epoch, baseline, account history, `ledger_sequence`, and state digest to the continuous input-WAL prefix, private `connection_epoch`, and `gap_generation`. The qualifying parent query must precede all three current remote snapshots (account, open orders, conditional orders). Because committing tag `16` increments `ledger_sequence`, closing a second parent requires collecting fresh snapshots against the updated ledger.
3. **Scoped effect and late-child revocation**: Synchronizing `UncreatedAlgoClosed` releases only the uncreated leg's order liability and marks its fees closed without modifying cash balances, actual positions, venue shadow capital, or the account's `frozen` state. Exact historical retries return `Applied::Duplicate`, while conflicting bindings halt and freeze. If a late child report later contradicts the closure, `Account` revokes `uncreated`, restores remaining liability from the original specification, sets `observation_unreconciled`, retains the tag-`16` record for audit, and freezes the account. On restart, recovery re-verifies the 4,096-byte closure object and input prefix, and the reconciliation controller acquires fresh snapshots before `Reactivated`.

## Verification and References

```sh
cargo test --locked -p tickwire-account --test conditional
cargo test --locked -p tickwire-binance-account
```

* Account and codec modules: [conditional structure reducer and v2/v3 digests](../../crates/tickwire-account/src/structures.rs), [structure admission checks](../../crates/tickwire-account/src/admission.rs), [canonical venue events](../../crates/tickwire-venue/src/lib.rs), [tag-16 account codec](../../crates/tickwire-account-codec/src/codec.rs)
* Binance provenance and closure verifiers: [REST structure normalization](../../crates/tickwire-binance-account/src/rest/structure.rs), [private stream structure normalization](../../crates/tickwire-binance-account/src/structure.rs), [read-only parent queries](../../crates/tickwire-binance-account/src/reconciliation/structures.rs), [trade page coverage](../../crates/tickwire-binance-account/src/reconciliation/trades.rs), [uncreated-algo closure verifier](../../crates/tickwire-binance-account/src/reconciliation/structure_closure.rs)
* Test suites: [account conditional-order tests](../../crates/tickwire-account/tests/conditional.rs), [uncreated-algo closure tests](../../crates/tickwire-binance-account/src/reconciliation/structure_closure/tests.rs)
* Related specifications: [multi-asset account state machine](account.md), [account gateway](account-gateway.md), [account reconciliation and terminal fee maintenance](reconciliation-controller.md)
