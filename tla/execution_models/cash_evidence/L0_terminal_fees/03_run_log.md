# TLC runs

TLC 2.19, one worker, breadth-first search, polynomial 0, pinned repository jar. The primary profile explores one trade and monotone terminal evidence, and the second allows two trades and conflicting evidence.

| Configuration | Generated | Distinct | Depth | Pending | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| [Primary](04_evidence/L0_TerminalFees.log) | 977 | 532 | 11 | 0 | Cash once, qualification, readable restart passed |
| [Two trades](04_evidence/L0_TerminalFees_two.log) | 29,355 | 6,986 | 13 | 0 | Cash once and qualification passed |
| [Conflicting terminal, readable restart](04_evidence/prefix_failure.log) | 1,651 | 596 | 9 | 25 | TLC stopped at the replay violation |

From the repository root, run `./scripts/check-tla.sh cash_evidence` for the two completed searches. The final coverage aggregates contain Init and all 12 Next actions. [Action counts](04_evidence/coverage.json)

To reproduce the failing property, run from this level's directory.

```sh
java -XX:+UseParallelGC -cp ../../../tools/tla2tools.jar tlc2.TLC \
  -workers 1 -fp 0 -coverage 1 -config 04_evidence/prefix_failure.cfg \
  02_models/L0_TerminalFees.tla
```

The diagnostic run should exit with a violation of `RestartReadable`. A successful test invocation of `04_evidence/reproduce.sh` means both real runner processes reproduced the expected rejection.
