# Finite model of book state and coverage

The archived TLC runs check two aspects of one exclusively owned book. L0 transcribes the Spot and USD-M sequence rules, stale events, bridge/`pu`, generation changes, atomic rejection and freshness at view creation. L1 keeps hidden complete books consistent with the accepted observation prefix. A reported known quantity or best price must agree with the witness. For an unknown quantity, two compatible complete books can give different answers. A live book may therefore have an unknown best price. [Book contract](book-core.md)

| Archived configuration | Generated | Distinct | Depth |
| --- | ---: | ---: | ---: |
| L0_BookState | 111,457 | 1,392 | 6 |
| L0_SpotScaling | 3,042,646 | 8,499 | 6 |
| L0_UsdM | 115,809 | 1,880 | 6 |
| L0_UsdMScaling | 2,220,496 | 10,524 | 6 |
| L1_BookCoverage | 45,523 | 1,471 | 6 |
| L1_Ask | 45,523 | 1,471 | 6 |
| L1_AskScaling | 585,796 | 8,244 | 6 |
| L1_BidScaling | 519,202 | 7,050 | 6 |

All eight finite configurations passed. The four L0 configurations exercised 19 Next actions. The four L1 configurations exercised 15. The archived source digest is `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`. The audit checked 425 source anchors. See each output for finite domains, assumptions and coverage. [L0 results](../../tla/execution_models/book_state/L0_lifecycle/05_output.md) · [L1 results](../../tla/execution_models/book_state/L1_coverage/05_output.md) · [Model plan](../../tla/execution_models/book_state/PLAN.md)

`scripts/check-tla.sh` drives the repository checks. Model freshness applies when a view is created. It does not update a previously returned view. The model covers neither the fixed-array implementation nor arbitrary custom sequence policies, all possible u64 values, a live feed, a full refinement proof or downstream risk authorization. The Binance book tests exercise its bounded Rust implementation. Bybit and OKX BBO snapshots have an independent contract and are outside these two models.
