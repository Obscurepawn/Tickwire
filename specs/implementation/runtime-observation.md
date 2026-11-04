# Querying a running strategy

A controller can ask a managed worker to checkpoint its current state while callbacks continue. Host issues a fresh challenge on the authenticated report session and checks the answer against the actual child, its report cursor and durable recovery objects. Consumers can inspect the parameters and inputs of that instance at the observed cut. [Parameter consumer](strategy-parameter-consumption.md) · [Input consumer](strategy-input-consumption.md)

## Checkpoint protocol

`CHECKPOINT_SEAL` and `CHECKPOINT_SEALED` retain their original wire layouts. Mode 0 captures an exact account waterline. Mode 2, available only on an approved SDK build, treats `report_sequence` as a lower bound. The reply echoes the requested position and the new READY carries the actual position, which may be greater.

The worker continues processing market, account, timer and external inputs until it reaches that floor. It checkpoints once and keeps the same reply under socket backpressure. An expired query is discarded. Periodic READY announcements use one replaceable slot, wait behind requested replies, and can still advance after the query with no further account input. Installation, release, feed changes, retirement and query challenges serialize on the session.

`ObservationRequest` supplies a request ID, an absolute deadline in the same monotonic domain and a flag requiring resource evidence. Host exposes begin, poll and cancel. One owner accepts at most 4,096 distinct request IDs and retains up to 16 closed, unanswered challenges so a late reply can be matched and discarded. Timeout releases session exclusion. When the bounded history is full the next request fails. During the query Host retains at most 4,096 actual account prefix proofs. Exhaustion fails that query while report publication continues. Only the explicit cold completion path reads the full recovery closure. [Host API](../../crates/tickwire-strategy-host/src/observation.rs)

## Evidence returned to the caller

Host observes the registry-owned child before and after reading. The challenge binds its actual boot ID, PID, start ticks, instance, seal sequence, control epoch and request ID. Both samples must agree on the child, image and credentials. The authenticated session, shared cursor and original deadline must remain valid after kernel and disk reads. A resource-required request also needs a real resource observation of that child.

A successful `FreshObservation` carries the challenge, original request, deadline, control epoch, current READY, actual prefix digest, cursor identity, process facts, optional resource facts and start/completion times. Each query generates a new challenge and READY sequence, even with unchanged business state.

* `configuration()` extracts the private configuration at the current merged cut, including its source, checkpoint, revision and original bytes. It validates worker identity, image, logical clock and business state. A legacy closure entirely without merged input yields `None`. A partial or malformed history fails.
* `inputs()` inspects the saved accounts, market replicas and report writer at that same cut. The full JOIN evidence must match the current bank to report its catalog and set version. A structurally valid checkpoint with no account or several accounts can lack inputs in this single-report Host. It never fills missing facts from a launch template.

A past READY, closed challenge or restarted instance cannot produce a fresh answer for the old request.

## App inbox and persistence

`Deployment.observation` enables distinct absolute private inbox, result and journal directories for an approved UID. Directories must be mode 0700, files regular and mode 0600. Symlinks, hard-link replacement, ownership/permission drift and oversized content close the affected operation. A new App session is published durably at launch. Each request binds it to the current `control::Binding`, exact logical strategy/instance/generation, request ID, deadline and resource requirement. Directory permissions authorize the local caller. There is no remote authentication endpoint for this inbox.

App syncs the request before sending the challenge. After verification it stores and deduplicates exact binary dependencies by digest, records sorted references and READY roots, syncs the terminal journal entry and publishes the result file. Recovery checks lengths, digests, ordering and required roots. An identical request reuses its prior outcome. A different body under an in-flight ID permanently rejects that ID and cancels the query. On restart, incomplete requests become `Interrupted` and completed observations retain their original time and deadline. Replacing the lock file or losing the store closes the observation service. Diagnostic storage failure does not change the trading authority.

| Limit | Maximum |
| --- | ---: |
| Historical requests / simultaneous active requests | 1,024 / 32 |
| Requests advanced / inbox files checked per scan | 16 / 16 |
| Result JSON / combined store budget | 256 KiB / 64 MiB |
| Journal entries / distinct nonzero approved build digests | 8,192 / 128 |
| Request lifetime / minimum scan interval | 30 s / 10 ms |

`AppStrategy` routes by exact instance and generation. Idle polls avoid kernel and recovery reads. Immediately before and after result publication App resamples time and rechecks the binding. Archived process tests cover consecutive queries, an actual account position past the requested floor, timeouts, paused/exited children, stale replies, file recovery and continued trading input. The host lacked a delegated cgroup for a positive whole-group test. [Runtime report](../evidence/20261029/runtime-observation.json) · [Acceptance verifier](../evidence/20261029/verify-runtime-observation-acceptance.py)
