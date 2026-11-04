# TLC runs

Run `./scripts/check-tla.sh cash_evidence` from the repository root. TLC 2.19 exhausted each configuration with one worker and an empty queue. The 13 action entries (including initialization) all had successors across the four runs. [Coverage and model digest](04_evidence/coverage.json)

| Profile | Generated | Distinct | Depth | Expected behavior |
| --- | ---: | ---: | ---: | --- |
| [One trade](02_models/L1_TerminalFees_v2.cfg) | 1,224 | 619 | 11 | Conflicting private and REST terminal totals can occur. Every new fee record passes strict replay. |
| [Two trades](02_models/L1_TerminalFees_v2_two.cfg) | 20,106 | 6,971 | 13 | Repeated executions debit once. Qualification requires consistent cash and quantities. |
| [Legacy disagreement](02_models/L1_TerminalFees_v2_legacy.cfg) | 2 | 2 | 2 | A rejected historical local decision permits frozen restart. |
| [Unexplained closure](02_models/L1_TerminalFees_v2_invalid.cfg) | 2 | 2 | 2 | The seeded journal fails replay. |

The [Bybit and OKX process test](../../../../crates/tickwire-spot-runner/tests/reconcile_process.rs) runs signed loopback REST and private WebSocket servers. It supplies the original contradictory totals, then the missing execution, fee and matching wallet. Each account reconciles after a gap and again in a restarted process with a new writer. Run `cargo test --locked -p tickwire-spot-runner --test reconcile_process real_reconcile_process`.
