# Per-order `QueryDesk`, non-freezing rejection, and single-order doubt isolation models

| Level | Result |
| --- | --- |
| [L0 per-order doubt isolation & non-freezing rejection](L0_per_order_isolation/05_output.md) | Locked cash/`shadow_holds`/`reduce` exposure while in doubt, healthy sibling trading across single-order transport drops and deterministic `VenueEventBody::Rejected`, grace-gated `CloseUncreated`, contradicted-`uncreated` reopening + freeze, shadow-preserving eviction, and durable WAL replay equivalence passed across primary and adversarial configurations. |
| [L1 `QueryDesk`, `PerOrderQueryDriver`, & concurrent WS convergence](L1_query_convergence/05_output.md) | Single-ask / single-slot per-order invariants across concurrent WS `remove_order`, zero-byte `on_not_sent` budget preservation, `Trades`-before-`FeesReconciled` closure, `cancel_after` + `trades_after` coalescing, Bybit two-step `realtime -> history` soundness, and bounded `MaxQueryCount` / Category B hazard freeze passed across Bybit two-step and `OpenPartFill` coalescing configurations. |
