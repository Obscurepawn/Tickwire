# Conditional Orders, Parent Identity, and Leg Liability

Multi-leg Spot order lists (`SpotList`: OCO, OTO, OTOCO) and USDⓈ-M conditional algorithms (`Algo`) separate parent coordination state from child order execution on the exchange. `tickwire-account` mirrors that separation directly: admitting a conditional command creates one immutable parent `Structure` alongside up to three potential leg `Order` entries, where each leg owns its own principal or reduction liability and the parent reserves nothing a second time. When a leg triggers on the venue, the resulting native child order binds onto its pre-admitted leg rather than allocating unbudgeted exposure.

Keeping parent lifecycle observations distinct from leg execution facts prevents premature reserve release. On both Spot and USDⓈ-M, a parent list or algo order can report a terminal status while a triggered child order still has unbooked fills; only per-child order observations and matching fills—or a verified `UncreatedAlgoClosed` proof that a USDⓈ-M algo never spawned a child—can close leg principal and reduction liabilities.

## State Model and Encoding Bounds

| Layer / Artifact | Schema, Tag, or Bound | Structural Contract |
| --- | --- | --- |
| Conditional admission | `1` parent (`[u8; 16]`), `1..=3` legs (`MAX_ADMISSION_ORDERS = 3`) | Fixes parent `StructureKind`, `command_digest`, and ordered leg `OrderKey`s (`[above, below]` for OCO, `[working, pending]` for OTO, `[working, pendingAbove, pendingBelow]` for OTOCO). |
| Structure observation | `CanonicalEvent` schema `2`, body tag `5` | Validates `Product`, original intent and execution epoch, parent identity, member count cap, and child uniqueness. |
| `UncreatedAlgoClosed` | Account record tag `16` (`13` fields) | Binds `scope`, `account_epoch`, `ledger_sequence`, `baseline_digest`, `history_digest`, `input_stream`, `through_sequence`, state/coverage digests, `structure_id`, original `intent`, `command_digest`, and native parent ID. |
| State-digest versions | Outer `v1` → `v2` (`conditional-state.v1`) → `v3` (`conditional-state.v2`) | Stays at `v1` until the first structure observation (`v2`), and advances to `v3` upon the first tag-`16` closure. Parents sort by `structure_id`, legs keep command order, children sort by `ClientId`, integers encode little-endian, and text is length-prefixed. |

## Core Invariants

* **Terminal parents never close child liabilities**: Parent acknowledgement, trigger, terminal, and unknown transitions apply as independent structure facts (with exact duplicates leaving `ledger_sequence` unchanged), but a terminal parent response, Spot member list, or USDⓈ-M aggregate `actualQty` never synthesizes a child trade or releases leg principal reservations. Every created child requires its own native order observation and matching fill quantities.
* **All-leg ownership checks on cancellation**: `CancelList` and `CancelAlgo` verify scope, instrument, original parent identity, and *every* leg's current `ManagerIdentity` and `owner_revision` at admission and at `check_admission`. Applying a `StrategyHandover` installs the updated manager and revision across affected legs.
* **Contradicted aborts restore bounded leg liability**: A parent acknowledgement forbids any subsequent local zero-byte abort. If an acknowledgement, fill, or fee arrives *after* a local abort, `Account` restores leg principal and derivative reduction reservations from the original `OrderSpec` (`original_quantity - booked_filled`, incorporating the new fill where applicable—never enlarging reduction size or switching side or position mode) and freezes the account pending reconciliation. Conflicting native parent or child IDs retain the original binding and freeze the account.
* **Permanent audit trail in conditional state digests**: Advancing outer account-input and strategy-order digests to `v3` (`tickwire.conditional-state.v2`) permanently hashes uncreated-leg closure state bits and closure digests into the state preimage, even if a late exchange child observation later revokes the uncreated flag and restores leg liability.

## State Transitions and Failure Semantics

### Child Correlation and Uncreated-Leg Closure (`UncreatedAlgoClosed`)

* **Spot order lists**: Member lists establish parent-to-leg membership, while per-leg lifecycle and fill status come exclusively from individual order and execution reports.
* **USDⓈ-M algo orders**: Every native child order binds through its registered parent leg; previously bound native IDs can never be reassigned to another leg.
* **Closing an untriggered USDⓈ-M algo (`Account::close_uncreated_algo`)**: When a USDⓈ-M conditional parent terminates (`CANCELED`, `EXPIRED`, or `REJECTED`) without spawning a child order, applying `UncreatedAlgoClosed` verifies scope, `account_epoch`, `ledger_sequence`, state digest, confirmed terminal non-triggered parent state, and zero fills/fees on the single leg, releasing only that uncreated leg's order liability (`uncreated = true`, `fee_reconciled = true`). Exact retries return `Applied::Duplicate`, while conflicting bindings or late child contradictions revoke `uncreated`, restore remaining liability, latch `observation_unreconciled`, and freeze `Account`.

## Verification and References

```sh
cargo test --locked -p tickwire-account --test conditional
```

* Account and codec modules: [structures.rs](../../crates/tickwire-account/src/structures.rs), [admission.rs](../../crates/tickwire-account/src/admission.rs), [lib.rs](../../crates/tickwire-venue/src/lib.rs), [codec.rs](../../crates/tickwire-account-codec/src/codec.rs)
* Test suites: [conditional.rs](../../crates/tickwire-account/tests/conditional.rs)
* Related specifications: [Stateless account and OrderManager](account.md), [Account baseline and record codec](account-codec.md), [Venue domain types](venue.md)
