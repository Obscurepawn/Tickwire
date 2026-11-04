# Tickwire specifications

Start with the [single-host design](implementation/single-host-trading.md) to follow a quote to a strategy and an order back to the venue. It includes the process boundaries, public APIs and cost of each step. The [exchange support matrix](../README.md#exchange-support) lists the paths available for each venue.

| Work on | Read | What to verify |
| --- | --- | --- |
| Market data | [Public spot quotes](implementation/spot-quote-input.md), [order books](implementation/book-core.md), [catalog](implementation/instrument-catalog.md) | Native price units, sequence continuity, snapshot and feed health |
| Multiple strategies | [Subscriptions](implementation/strategy-input-consumption.md), [parameters](implementation/strategy-parameter-consumption.md), [market JOIN](implementation/join.md) | A shared feed, an independent cursor per worker, and attribution of every command |
| Orders and account state | [Spot adapters](implementation/spot-execution.md), [shared account](implementation/spot-strategy-routing.md), [gateway](implementation/account-gateway.md), [risk](implementation/risk-core.md) | Available funds at admission, first socket write, duplicates, partial fills and fees |
| Restart and storage | [Account journal](implementation/account-journal.md), [Binance reconciliation](implementation/reconciliation-controller.md), [Bybit and OKX recovery](implementation/spot-rest-recovery.md) | One writer per journal directory and preservation of pending exposure |
| Local configuration | [Candidates and projections](implementation/config-candidate.md), [materialization](implementation/config-materializer.md) | Process identity, permission scope, configuration bytes and local receipts |

See the [module index](implementation/README.md) for individual contracts, [wire and configuration schemas](contracts/README.md) for encoded formats and the [test guide](../scripts/README.md) for commands. The [October 28](evidence/20261028/README.md) and [October 29](evidence/20261029/README.md) archives identify the revisions and fixtures used for earlier acceptance runs.
