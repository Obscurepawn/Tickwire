# October 20 design for Binance Spot and USDⓈ-M

On October 20, 2026 Tickwire accepted v0.3 as the design for a Rust trading system covering Binance Spot and USDⓈ-M futures. A strategy could be added, upgraded or rolled back while the account owner kept its exchange connection and continued to manage outstanding orders. The deliverables at that date were the architecture, machine-readable contracts, deployment profile and verification plan. Subsequent Linux runs and their limits are recorded separately. [Implementation record](20261020-implementation.md) · [Current system design](implementation/single-host-trading.md)

## Ownership and data flow

| Process | Owns | Communicates through |
| --- | --- | --- |
| Market-data producer | Exchange session, snapshot and incremental sequence, normalized market state, one writer per topic | Publishes one copy to a shared-memory broadcast ring. Readers take independent cursors. A recorder uses its own quality-of-service relay. |
| Strategy instance | Strategy parameters, private state, checkpoints and its input position | Reads the market ring and account reports. Writes intents through its own producer/consumer channel. Only an authorized generation can cause an external action. |
| Account execution owner | One risk domain's credentials, orders, reservations, balances, positions, reconciliation and outbound connection | Receives intents, commits the decision, calls the exchange, applies private reports and publishes account updates. Strategy code runs in a separate process. |
| Supervisor and guardian | Child process identity and lifecycle, observation, and evidence for isolating the old sender | Coordinate startup, retirement and recovery. Execution authority can transfer only after the recovery and fencing checks complete. |

The ring uses a fixed interprocess ABI. Publishing exposes a contiguous prefix. A live borrow or unread record prevents slot and payload reuse. On a slow reader the owner can freeze the old bank, move healthy readers to a successor and reclaim the previous allocation after proven quiescence or exit. Switching preserves the sequence from S to S+1 or requires an explicit snapshot and resynchronization. Exhausted capacity closes the affected topic or relay according to its policy. [IPC wire contract](contracts/ipc-wire-v1.json) · [Ring implementation](implementation/shm-ring.md) · [JOIN protocol](implementation/join.md)

During a rollout the old and shadow instances can read the same inputs. Execution transfers management of existing orders and positions in place, retains outstanding reservations and rejects stale generations. Multi-domain activation requires the authorized domains to agree before opening the new generation. Rollback follows the same ownership rules. [Strategy handover](implementation/strategy-handover.md) · [Multi-domain coordinator](implementation/lifecycle-coordinator.md)

## Execution and recovery invariants

The original fourteen invariants provide the acceptance vocabulary for protocol, account and failure tests.

| IDs | Required behavior |
| --- | --- |
| INV-1, 2 | Before increasing risk, cover outstanding reservations and the resulting exposure. Record an actual shortfall after an external change. An intent that may have been sent holds its reservation until authoritative evidence resolves it. |
| INV-3, 4 | A distinct exchange fill changes economic state once. Late acknowledgements cannot reduce cumulative fills or reopen a terminal order. Apply an exchange correction as a separate adjustment. |
| INV-5, 7 | Require fresh market and account state to increase risk. After a data gap, crash or private-stream loss, close the affected gate until reconciliation and any required fencing complete. |
| INV-6, 12 | Each risk domain has at most one effective sender. Validate the strategy generation and domain grant on every external action. A new local epoch cannot isolate a still-running sender. |
| INV-8, 9, 10 | Bind decisions to matching instrument and risk versions. Keep Spot assets and futures margin separate until a transfer settles. During a freeze, verify a reduction against the actual position mode and pending orders. |
| INV-11, 14 | Preserve live shared-memory references, cursor ownership and stream continuity across bank changes. Reject incompatible ABI or generation before data reaches a trading decision. |
| INV-13 | Strategy replacement preserves positions, order identity and unknown reservations. An already-sent order stays with the same economic record. |

The default durability profile writes the intent and all recovery dependencies locally before the first send. A mirror copies the contiguous journal in the background. Control-plane Raft coordinates configuration, releases and authority. If the primary is lost with an unproven tail, recovery enters `EvidenceLost/Frozen`. Cross-host takeover requires a complete source or mirror seal, physical isolation of the former sender and venue reconciliation. An explicitly configured `SyncReplica` also waits for the designated mirror's durable acknowledgement and closes new risk if that mirror is unavailable. [Replication profiles](contracts/replication-profile-v1.json)

Spot risk tracks resources and fees per asset. The USDⓈ-M design also accounts for position mode, exposure after pending orders, margin, leverage tiers, exit fees and funding. Arithmetic uses checked fixed-point units with conservative rounding. Each deployment must supply actual account capabilities, capital, fees, instrument rules and market sources. [Risk vectors](contracts/risk-vectors-v1.json) · [Production template](../config/production.template.toml)

The configuration API binds an operation to its base revision, actor, intended projections and idempotency key. A successful apply requires receipts for both the effective runtime state and the locally persisted file at the same revision and hash. The contract covers planned successors, rollback, projection permissions and directory migration after a crash. [Configuration schema](contracts/system-config.schema.json) · [Control API](contracts/control-api.openapi.json)

## Reference capacity and latency targets

The reference profile allocates two trading hosts in separate failure domains, each with 64 physical cores, 256 GiB RAM and 25 Gb/s networking, plus a third control voter. The capacity test calls for two market topics, two execution domains and 16 active plus 16 shadow strategies. Each strategy receives both topics. The offered load is 100,000 market events/s in total, bursts at three times that rate for 100 ms, and 1,000 synthetic order intents/s. These are qualification inputs. [Deployment profile](contracts/deployment-reference-v1.json)

| Qualification target | p99 | p99.9 |
| --- | ---: | ---: |
| IPC | 2 µs | 5 µs |
| Core step | 10 µs | 25 µs |
| Complete tick to socket, local durability and asynchronous mirror | 300 µs | 1,000 µs |
| Complete tick to socket, synchronous mirror | 2,000 µs | 5,000 µs |

Complete latency runs from the complete plaintext market message to acceptance of all order-related ciphertext by the local kernel. It includes scheduling, IPC, strategy work, risk, recovery dependencies, journal flush and socket backpressure. Measure that interval directly. The protocol calls for a ten-minute warmup, a one-hour baseline, a 72-hour soak, at least one million samples for p99.9 and accounting for rejected or delayed attempts. Venue acknowledgement is a separate interval. The 30-second recovery target assumes an intact source or a demonstrably complete mirror. The one-second handoff target starts after a compatible shadow is ready under the declared load. [Measurement and recovery contract](contracts/deployment-reference-v1.json)

## Design acceptance and reproduction

At acceptance the ABI had 36 layouts, 624 fields and 63 golden vectors. The risk suite had 21 numerical cases with 131 expected results. The API specified 16 operations. The checker covered 17 negative configuration cases and validated OpenAPI. The verification plan listed 30 unit/property cases and 65 integration cases. No trading engine, process qualification, TLC run or measured end-to-end percentile formed part of the October 20 sign-off. The archive retains the original protocol and source research, including the pinned NautilusTrader, Barter and hftbacktest revisions. [Original design and research](https://github.com/Obscurepawn/Tickwire/tree/97a493c24549a8b6f055a4aabebcd3096c394ee8/docs) · [Original checker output](https://github.com/Obscurepawn/Tickwire/blob/97a493c24549a8b6f055a4aabebcd3096c394ee8/docs/validation/design-contract-check.json)

Run the current contract suite from the repository root with the Python dependencies in the script guide. It reports the counts for the checked-out revision. Deployment still requires the actual host and NUMA inventory, credential references, account facts, storage power-loss evidence, fencing backend and qualification report. [Verification setup](../scripts/README.md) · [Reference configuration](../config/reference.toml)

```sh
python3 scripts/verify-design-contracts.py --openapi
python3 scripts/verify-documentation.py
```
