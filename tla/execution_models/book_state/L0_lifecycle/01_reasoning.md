# L0 modeling rationale and abstractions

Verifies that `Book` never exposes invalidated or half-written side content across snapshot installation, delta bridging, continuous updates, error rejections, and generation rebuilds. Malformed batches, stale updates, clock regressions, and explicit invalidations are modeled directly as public API inputs.

| Preserved semantics | Abstraction and role |
| --- | --- |
| Spot / USD-M branch order and `U`/`u`/`pu` comparisons | Fixed `BookKey` with mismatched identity abstracted to inequality; `None` represented as `-1` |
| `generation`, four lifecycle states, watermarks, and last-accepted timestamp | Four `Invalid` diagnostic reasons merged because subsequent lifecycle behavior is identical |
| Atomic two-sided snapshot/delta commit and content preservation on failure | Independent `bid` and `ask` content witnesses updated on valid commit and preserved on `fail` |
| `view` checks for `Live`, negative age, and `max_age_ns` | `viewNow` records the latest accepted `view` call timestamp without background auto-expiry |
| `begin_rebuild` clears content, timestamps, and watermarks | Single `Book` owner with atomic `&mut Book` call boundaries |

Level sorting, deduplication, capacity, and crossed-book checks collapse into `PayloadError` / `SnapshotError`, while finite price-level knowledge is refined in `L1_coverage`. Primary configurations check `generation \in 0..1`, `ID \in 0..2`, `time \in 0..2`, 2 side witnesses, and `max_age = 1`; scaled configurations expand to `generation \in 0..2`, `ID/time \in 0..3`, 3 side witnesses, and `max_age = 0` on USD-M.

All 19 `Next` actions cover valid and error branches of the five stateful methods (`install_snapshot`, `apply`, `begin_rebuild`, `invalidate`, `view`). Six action-safety invariants verify that failed calls never mutate content, wrong identities leave `Book` untouched, stale deltas do not refresh timestamps, `generation` never regresses, `Invalid` exits only via `begin_rebuild`, and accepted delta watermarks never regress, alongside creation-time `Live` and freshness invariants on `view`.
