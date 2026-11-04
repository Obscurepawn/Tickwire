# Economic reducer and restart

Each action models one serialized reducer call. An order reserves two units of quantity plus a fee allowance. `RecordFill`, `RecordFee` and `RecordTerminal` append the fact and attempt to apply it. Duplicate observations are idempotent. Conflicting facts or a full event table freeze new exposure. A terminal message can arrive before trades or fees.

Restart freezes the old engine, constructs a new engine from the journal, then installs that engine after successful replay. The arithmetic domain uses 1,000 as a representative upper bound for selected checked operations near `i128::MAX`. The configurations cap the number of facts and restarts. [Detailed abstraction and upper-bound argument](https://github.com/Obscurepawn/Tickwire/tree/04351dbb59522c43d6b7a1b27366c4a2ee89ed9c/tla/execution_models/submission/L2_economic_facts/01_reasoning.md) · [Model](02_models/L2_EconomicFacts_v2.tla)
