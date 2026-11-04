# Management isolation results

Run `./scripts/check-tla.sh strategy_intake` from the repository root. The v2 runs used the repository's pinned TLC 2.19 with one worker. All three searches ended with an empty queue.

| Configuration | Generated | Distinct | Depth |
| --- | ---: | ---: | ---: |
| [One cash unit](evidence/L1_SharedCash_v2.log) | 632 | 492 | 10 |
| [Two units, one restart](evidence/L1_SharedCash_v2_restart.log) | 4,412 | 2,994 | 17 |
| [Original failing configuration](evidence/L1_SharedCash_v2_witness_replay.log) | 858 | 698 | 13 |

The third configuration is byte-identical to the [original probe](../L0_shared_cash/evidence/PeerCanContinue.cfg). All 12 Next actions have successors across the runs. The [action totals](evidence/action-coverage.json) include the separate unknown-origin and management-refusal branches.

The [reachability probe](evidence/reachable.cfg) deliberately rejects a state containing the sibling’s later cancel ACK. The [11-state witness](evidence/reachable.log) reaches the refusal at step 8 and the ACK at step 11. To reproduce it, copy the model and this configuration to a scratch directory and run `java -cp /absolute/path/to/tla2tools.jar tlc2.TLC -workers 1 -fp 0 -seed 1 -config reachable.cfg L1_SharedCash_v2.tla`. TLC exits 12.
