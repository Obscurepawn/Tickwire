# Implementation contracts

Choose a subsystem below or open the [workflow index](../INDEX.md). Each specification describes a code boundary and the evidence used to check it.

## Order flow and risk

Start with the account and its gateway. The other contracts define admissible commands, economics, recovery dependencies and transport.

* [Single-host trading](single-host-trading.md) · [Account](account.md) · [Account gateway](account-gateway.md) · [Spot execution](spot-execution.md) · [Shared spot strategy account](spot-strategy-routing.md)
* [Venue](venue.md) · [Risk core](risk-core.md) · [Execution core](execution-core.md) · [Gateway](gateway.md)
* [Conditional orders](conditional-orders.md) · [Spot amend](spot-amend.md) · [USD-M account facts](usdm-account-facts.md) · [Command binding](command-binding.md)
* [Account recovery binding](account-recovery-binding.md) · [Reconciliation controller](reconciliation-controller.md) · [Runtime policy](runtime-policy.md) · [Mock abort](mock-abort.md)

## Public market data and exchange protocols

Read the venue protocol next to its consumer. Use the book contract for sequencing and the snapshot contract for JOIN recovery.

* [Spot quote input](spot-quote-input.md) · [Book core](book-core.md) · [Book state model](book-state-model.md) · [Instrument catalog](instrument-catalog.md)
* [Instrument index](instrument-index.md) · [Market expansion](market-expansion.md) · [Binance protocol](binance-protocol.md) · [Binance streams](binance-streams.md)
* [Binance depth sync](binance-depth-sync.md) · [Binance public session](binance-public-session.md) · [Binance market events](binance-market-events.md) · [Binance SBE](binance-sbe.md)
* [Binance execution](binance-execution.md) · [Binance private session](binance-private-session.md) · [Binance private clock adapter](binance-private-clock-adapter.md) · [Binance quota](binance-quota.md)
* [Binance request extensions](binance-request-extensions.md) · [Binance WebSocket trading](binance-ws-trading.md) · [Quota](quota.md) · [Clock](clock.md)
* [System clock](system-clock.md) · [Transport](transport.md)

## Processes, strategy state and IPC

Each worker joins a market topic with a snapshot and ring position, tracks its own cursor and reports its progress to the host.

* [Supervisor process](supervisor-process.md) · [IPC codec](ipc-codec.md) · [JOIN](join.md) · [Shared memory mapping](shm-mapping.md)
* [Shared memory ring](shm-ring.md) · [Immutable resource](immutable-resource.md) · [Topic snapshot](topic-snapshot.md) · [Topic snapshot builder](topic-snapshot-builder.md)
* [Strategy inputs](strategy-input-consumption.md) · [Strategy parameters](strategy-parameter-consumption.md)
* [Runtime observation](runtime-observation.md)

## Persistence and recovery

Follow a record from its canonical encoding to the local commit and the evidence used on restart.

* [Account codec](account-codec.md) · [Account journal](account-journal.md) · [Business journal](business-journal.md) · [Journal](journal.md)
* [Journal codec](journal-codec.md) · [Recovery closure](recovery-closure.md)

## Configuration and deployment

The configuration reader validates identity and bounds. The local materializer installs the accepted revision on disk.

* [Configuration candidate](config-candidate.md) · [Plan encoding](config-plan-archive.md) · [Locator](config-locator.md) · [File store](config-file-store.md)
* [Journal store](config-journal-store.md) · [Local journal](config-local-journal.md) · [Materializer](config-materializer.md)
* [Local control sessions](control-session.md) · [Unix transport](control-transport.md)
