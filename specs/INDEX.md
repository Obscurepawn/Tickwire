# Tickwire specifications

Start with the [single-host trading design](implementation/single-host-trading.md) for the live order path, state ownership and the cost of each decision. The [module index](implementation/README.md) covers the complete system. Use the [machine contracts](contracts/README.md) when a change affects bytes, configuration identity or money.

| If you are changing | Read | Check |
| --- | --- | --- |
| A market-data adapter | [Quote inputs](implementation/spot-quote-input.md), [sequence policy](implementation/book-core.md), [normalized events](implementation/binance-market-events.md) | Exact price units, epoch changes, gap recovery, client-visible coverage |
| A strategy worker or its deployment | [Strategy inputs](implementation/strategy-input-consumption.md), [parameters](implementation/strategy-parameter-consumption.md), [worker handover](implementation/strategy-handover.md), [two-worker deployment](../README.md#deploy-two-strategies) | Per-instance identity, shared market source, checkpoint and output sequence |
| An order, cancellation or trade | [Spot adapters](implementation/spot-execution.md), [shared account runner](implementation/spot-strategy-routing.md), [account gateway](implementation/account-gateway.md), [risk calculations](implementation/risk-core.md) | First-write grant, duplicate intent, late fill and fee attribution |
| IPC or storage | [Ring](implementation/shm-ring.md), [JOIN](implementation/join.md), [account journal](implementation/account-journal.md) | Wire vectors, cursor continuity, crash before and after sync |
| Configuration or control | [Configuration candidate](implementation/config-candidate.md), [planning](implementation/config-planning.md), [service](implementation/config-server.md) | Agreement between committed revision, runtime observation and effective file |
| Cross-host recovery | [Replication](implementation/replica.md), [coordinator](implementation/lifecycle-coordinator.md), [guardian](implementation/guardian.md) | Ownership transfer, fencing evidence, catch-up frontier and recovery grant |

The [October design](20261020-binance-ultra-low-latency.md) records the original Binance and HA profile. The [implementation record](20261020-implementation.md) tracks the scenarios and limitations established for that revision. Dated evidence lives under [October 28](evidence/20261028/README.md) and [October 29](evidence/20261029/README.md). Historical benchmark values describe their recorded fixtures and machines.

See the [test commands](../scripts/README.md) and [formal models](../tla/README.md) for reproducible checks.
