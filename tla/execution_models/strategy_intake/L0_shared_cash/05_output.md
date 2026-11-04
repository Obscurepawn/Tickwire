# Shared cash, retry and response ownership

Two finite safety configurations exhausted their queues. A fresh buy reserves the shared balance once. A verified byte-identical retry cannot cause another native write. A denied zero-byte send can release its reservation. An ACK leaves the order's cash reserved and is routed back to the member whose queue supplied the original command. There is one separate [confirmed worker-isolation defect](06_review.md) in management refusal handling.

## Fidelity and coverage

The model checks two members, four possible command keys, two descriptors per member, one or two cash units, up to one process restart and one native request outstanding. There are 32 initial states in each configuration. All 11 actions generated successors across the safety runs. The distinct-state counts were 480 and 2,968. These are finite safety results for a cash new/cancel path. The runner gives no durable delivery receipt for a file queue callback after a process crash. Full account arithmetic, authenticated reconciliation, per-fill accounting, partial network writes and worker instance replacement are outside this model. [Source map](02_models/L0_SharedCash.tla) · [Run evidence](03_run_log.md)
