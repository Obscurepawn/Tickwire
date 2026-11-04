# Implementation contracts

Choose a subsystem below or open the [workflow index](../INDEX.md). Each specification describes a code boundary and the evidence used to check it.

## Order flow and risk

Start with the account and its gateway. The other contracts define admissible commands, economics, recovery dependencies and transport.

* [single host trading](single-host-trading.md) · [account](account.md) · [account gateway](account-gateway.md) · [spot execution](spot-execution.md) · [shared spot strategy account](spot-strategy-routing.md)
* [venue](venue.md) · [risk core](risk-core.md) · [execution core](execution-core.md) · [gateway](gateway.md)
* [conditional orders](conditional-orders.md) · [spot amend](spot-amend.md) · [USD M account facts](usdm-account-facts.md) · [command binding](command-binding.md)
* [account recovery binding](account-recovery-binding.md) · [reconciliation controller](reconciliation-controller.md) · [runtime policy](runtime-policy.md) · [mock abort](mock-abort.md)

## Public market data and exchange protocols

Read the venue protocol next to its consumer. Use the book contract for sequencing and the snapshot contract for JOIN recovery.

* [spot quote input](spot-quote-input.md) · [book core](book-core.md) · [book state model](book-state-model.md) · [instrument catalog](instrument-catalog.md)
* [instrument index](instrument-index.md) · [market expansion](market-expansion.md) · [Binance protocol](binance-protocol.md) · [Binance streams](binance-streams.md)
* [Binance depth sync](binance-depth-sync.md) · [Binance public session](binance-public-session.md) · [Binance market events](binance-market-events.md) · [Binance SBE](binance-sbe.md)
* [Binance execution](binance-execution.md) · [Binance private session](binance-private-session.md) · [Binance private clock adapter](binance-private-clock-adapter.md) · [Binance quota](binance-quota.md)
* [Binance request extensions](binance-request-extensions.md) · [Binance ws trading](binance-ws-trading.md) · [quota](quota.md) · [clock](clock.md)
* [system clock](system-clock.md) · [transport](transport.md)

## Processes, strategy state and IPC

These contracts describe the live worker, the data a joining reader can see, and the state passed to a successor.

* [supervisor process](supervisor-process.md) · [IPC codec](ipc-codec.md) · [join](join.md) · [shared memory mapping](shm-mapping.md)
* [shared memory ring](shm-ring.md) · [immutable resource](immutable-resource.md) · [topic snapshot](topic-snapshot.md) · [topic snapshot builder](topic-snapshot-builder.md)
* [strategy handover](strategy-handover.md) · [strategy graceful retirement](strategy-graceful-retirement.md) · [strategy input consumption](strategy-input-consumption.md) · [strategy parameter consumption](strategy-parameter-consumption.md)
* [strategy rebuild](strategy-rebuild.md) · [strategy replay](strategy-replay.md) · [handover inputs](handover-inputs.md) · [release preflight](release-preflight.md)
* [runtime observation](runtime-observation.md)

## Persistence and recovery

Follow a record from its canonical encoding to the local commit and the evidence used on restart.

* [account codec](account-codec.md) · [account journal](account-journal.md) · [business journal](business-journal.md) · [journal](journal.md)
* [journal codec](journal-codec.md) · [recovery closure](recovery-closure.md)

## Configuration and deployment

Configuration plans bind a desired revision to a qualified runtime, observed state and a durable local result.

* [config candidate](config-candidate.md) · [config locator](config-locator.md) · [config file store](config-file-store.md) · [config journal store](config-journal-store.md)
* [config local journal](config-local-journal.md) · [config materializer](config-materializer.md) · [config control state](config-control-state.md) · [config planning](config-planning.md)
* [config runtime facts](config-runtime-facts.md) · [config credential facts](config-credential-facts.md) · [config archive sources](config-archive-sources.md) · [config plan archive](config-plan-archive.md)
* [config desired read](config-desired-read.md) · [config server](config-server.md)

## Control and optional high availability

These modules coordinate cluster control, group releases and cross-host replication.

* [control session](control-session.md) · [control transport](control-transport.md) · [lifecycle coordinator](lifecycle-coordinator.md) · [control Raft](control-raft.md)
* [control Raft service](control-raft-service.md) · [replica](replica.md) · [guardian](guardian.md)
