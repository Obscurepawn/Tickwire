# Verifying a strategy's account and market subscriptions

`HostConfig.projection_inputs` verifies that the running worker actually consumed the full account and market scope approved by `strategy.account_ids` and `strategy.topic_ids`. The supported launch has one account report stream and one external market topic per registered worker.

## Before launch

The deployment supplies a complete projection, `profile::Binding` and `Scope`, query deadline and explicit mappings from the account, execution and market producer names to their actual identities. Each mapping names the current owner. Strict JSON rejects extra or duplicate fields. App checks before creating directories or spawning children.

| Approved set | Runtime checks |
| --- | --- |
| Accounts | `account_ids`, `account_views` and the account mappings name exactly the same set. `venue_views` covers the referenced venues. Match the canonical baseline bytes and complete scope, account journal, execution route, command binding, bootstrap risk domain, intent/report peers and report producer. |
| Markets | `topic_ids`, `ipc.topics` and the mappings name exactly the same set. Match the authenticated `ConnectConfig`, bank descriptor, namespace, current producer, catalog and complete membership with full 64-bit instrument IDs and versions. Only the existing zero-descriptor template may be normalized. |
| Product and environment | Spot requires Spot, USDⓈ-M requires a linear perpetual or delivery contract, simulated requires Test and production requires Production. A normalized bank may have zero venue/product fields. Use the execution route, binding and canonical account instruments to establish each member's economics. |

If launch resources or private parameters are constrained too, every binding must name the same complete projection and scope. The input proof covers these subscriptions. Separate consumers own the economic catalog bytes, `allowed_symbols` and risk budgets. [Economic catalog](instrument-catalog.md) · [Parameter gate](strategy-parameter-consumption.md)

## Observing actual consumption

`replay::application::observed::inspect(&Checkpoint)` reconstructs the saved account and market views without calling user code. It checks a complete, unique stream list, encoded views, bank-to-stream bindings, account history and each runtime cursor. `FreshObservation::inputs()` exposes this current merged checkpoint as `ObservedInputs`. It carries the source, cut and checkpoint, then the following facts.

* Each account includes its canonical baseline bytes and scope, cursor, position and input digest. Host checks them against the sealed report history and its actual published prefix.
* Each market includes the current bank, producer instance and generation, cursor and member IDs and versions. `ObservedJoin` provides the catalog hash, instrument-set version, snapshot, baseline sequence and required states only when the seal names a matching original `TWSC0001` → `TWSB0001` JOIN and the snapshot verifies. A valid later bank without that evidence yields `join=None`.
* The report header and identity come from the real `ReportWriter`. Subscription and sequence come from its checked shared cursor. The complete MD peer identity comes from authenticated startup approval. An independent observation of the MD kernel process would require further evidence.

On a valid checkpoint with zero or multiple accounts, this single-report Host can return `inputs=None`. Malformed or partially present evidence fails. When the binding is enabled, App requires the complete matching facts. A joint parameter/input check uses one fresh challenge and the earlier deadline. It progresses no more often than every 10 ms, checks time again before publishing both proofs and holds all intents in the meantime. A recovered launch queries again. [Observation protocol](runtime-observation.md)
