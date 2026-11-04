# TLC results

The primary configuration covers one revision with up to one process stop. The scaling configuration covers two revisions and up to two stops. Both enumerate every eligible interruption and retry without scheduling fairness. The checker used the repository's pinned TLC 1.7.4, one worker and polynomial 0.

| Configuration | Generated | Distinct | Depth | States queued | Time | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| One revision | 576 | 199 | 17 | 0 | <1 s | Five invariants hold |
| Two revisions | 7,114 | 1,563 | 31 | 0 | 1 s | Five invariants hold |

Run `./scripts/check-tla.sh config_activation` from the repository root. The recorded run used `java -Xmx300m -XX:+UseParallelGC -cp <pinned-jar> tlc2.TLC -workers 1 -fp 0 -fpmem 0.05 -coverage 1 -metadir states -config <cfg> L0_FileReceipt.tla` in per-configuration scratch directories. [Primary excerpt](evidence/L0_FileReceipt.log) · [Scaling excerpt](evidence/L0_FileReceipt_scale.log) · [Validated action counts](evidence/coverage.json)
