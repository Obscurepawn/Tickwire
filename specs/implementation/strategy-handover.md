# Strategy process handover

The execution owner transfers management authority with one synchronized `StrategyHandover` account record. Existing orders retain their original ownership. Cash, fees, reservations, fills and uncertain sends continue through the same account journal.

## Retiring the previous owner

The host must obtain an `ExitObservation` for the child it owns. A failed `Registry::terminate` used to be ignored. The regression test exercises an empty registry to verify that a missing observation prevents success. When the old instance fails to retire, the strategy group keeps both instances paused and never invokes successor activation.

A stop request for a historical operation must match its complete gate binding. In two consecutive handovers, a late Halt or invalidation from the first operation cannot close the second operation's gate. If no gate exists, stopping is limited to the exact old or new instance grant named by the operation.

## Process acceptance

The Linux `spot-handover` laboratory starts an Active worker, acknowledges its order and holds back the fill. It then admits a new Shadow process and verifies that both workers consumed the same account prefix. The lifecycle CLI produces signed Prepare, Commit and Release messages. The relay preserves the signed bytes, publishes the new grant before forwarding Commit and waits for a real Committed receipt before creating Release. The execution owner drains the control inbox before processing the next lifecycle request.

Acceptance uses durable evidence. The old PID must exit, exactly one account record must transfer management authority, and the successor must observe the old order's late fill exactly once. The verifier checks the sequence of each fill recorded by either `VenueEvent` or `IngressEvent`. The first fill must follow the unique `StrategyHandover`. It also checks the explicit phases for new-intent admission and restart. A final balance or elapsed delay cannot establish this order.

```sh
./scripts/run-core-chain-linux.sh spot-handover release
```

An unconfirmed target replay installation closes the handover and preserves a rejection receipt. Inspect the receipt, the process logs and the account prefix when diagnosing a failed run. [Laboratory scenarios](../../crates/tickwire-lab/README.md) · [Strategy replay](strategy-replay.md)
