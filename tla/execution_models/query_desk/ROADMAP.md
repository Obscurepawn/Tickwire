# Per-Account ORS QueryDesk Levels

| Level | Engine | Result |
| --- | --- | --- |
| [`L0_ors_query_desk`](L0_ors_query_desk/05_output.md) | TLC (`--coverage`) | Safety PASS across unbounded (`MaxQueryCount = 0`) and opt-in bounded (`MaxQueryCount = 2`) configurations with 100% action coverage |
| [`L1_sym_ors_query_desk`](L1_sym_ors_query_desk/05_output.md) | Apalache (`inductive` + `bmc`) + TLC (`--coverage`) | Inductive `NoError` (`base`, `step`, `entailment`), BMC `NoError` (`length=5`), and TLC `BoundedMode` PASS with 100% action coverage |
