# Model checks

TLC 1.7.4, pinned jar SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`. All three runs used one worker, fingerprint polynomial 0 and the same Rust revision and TLA+ source. Coverage was enabled. Normal configs used a 720 MiB JVM heap, a 5% fingerprint reserve and parallel GC. A preliminary 400 MiB run with the default reserve exhausted memory before exploration. The repository runner also verifies the final action counts.

| Configuration | Generated | Distinct | Depth | Queued at finish | Wall time | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Two batches, time 1–3 | 713 | 447 | 10 | 0 | 1 s | Safety invariants hold |
| Three batches, time 1–3 | 14,825 | 8,288 | 15 | 0 | 4 s | Safety invariants hold |
| Two batches, subscription retention probe | 106 | 86 | 5 | 36 | 1 s | Counterexample |

From the repository root use `JAVA_TOOL_OPTIONS='-Xmx900m' ./scripts/check-tla.sh quote_delivery` for both positive configs. For the expected violation, copy `02_models/L0_QuotePair.tla` and `evidence/retention.cfg` together into a scratch directory. Run `java -Xmx400m -XX:+UseParallelGC -cp /absolute/path/to/tickwire/tla/tools/tla2tools.jar tlc2.TLC -workers 1 -fp 0 -coverage 1 -metadir states -config retention.cfg L0_QuotePair.tla`. The invariant-violation status is 12. The last column of the saved action coverage is generated successors. TLC reports the TRUE/FALSE wrappers of Publish as one parameterized action. [Aggregated action counts](evidence/coverage.json) · [Primary output excerpt](evidence/L0_QuotePair.log) · [Scaling output excerpt](evidence/L0_QuotePair_scale.log) · [Counterexample trace](evidence/retention.log)
