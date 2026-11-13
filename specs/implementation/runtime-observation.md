# Live Strategy Runtime Observation and Local Diagnostic Inbox

`tickwire-strategy-host::observation` and `tickwire-app::observation` enable a controller or local operator to capture a fresh, cryptographically bound checkpoint of a running strategy worker without pausing live market, account, or timer callbacks. On the authenticated report control session, the host issues a fresh `CHECKPOINT_SEAL` challenge (`opcode 39`, `seal_mode = 2` / `OBSERVE_CURRENT`) specifying the current report position as a lower bound, brackets durable recovery-closure reads between two independent kernel `/proc` observations of its owned `Child`, and exposes `FreshObservation`—from which callers inspect verified private parameters (`configuration()`) and live account/market subscriptions (`inputs()`).

Runtime observation owns challenge freshness, session serialization, kernel-to-checkpoint correlation, and optional local file-inbox persistence (`ObservationInbox`). It grants neither configuration `Applied`/`Effective` authority nor trading risk permission: exhausting observation request quotas or closing the diagnostic file store leaves live order/report execution authority untouched.

## State Model and Bounds

Each host readiness state machine (`ReadyProof`) serializes at most one active `Pending` query alongside installation, release, market-transition, and retirement operations on the session, while quarantining late replies from timed-out or cancelled challenges. When `Deployment.observation` is configured, `ObservationInbox` exposes three distinct `0700` local directories (`inbox`, `outbox`, `journal`) restricted to a single `owner_uid`.

| Scope | Parameter or Resource | Enforced Bound |
| --- | --- | ---: |
| `StrategyHost` (`fresh.rs`) | Single-use `ObservationRequest` IDs per owner (`IDS`) | `4,096` |
| `StrategyHost` (`fresh.rs`) | Quarantined unanswered challenges (`CLOSED`) | `16` |
| `StrategyHost` (`prefix.rs`) | Retained account-input prefix digests during an active query | `4,096` |
| `ObservationInbox` (`observation.rs`) | Historical requests (`maximum_requests`) / concurrent active (`maximum_pending`) | `1,024` / `32` |
| `ObservationInbox` (`observation.rs`) | Requests advanced / inbox files inspected per poll (`maximum_per_poll`) | `16` / `16` |
| `ObservationInbox` (`observation.rs`) | Request JSON (`MAX_REQUEST`) / Result JSON (`MAX_RESULT`) / Store budget (`MAX_TOTAL`) | `32 KiB` / `256 KiB` / `64 MiB` |
| `ObservationInbox` (`observation.rs`) | Journal records (`MAX_RECORDS`) / distinct nonzero `allowed_build_hashes` | `8,192` / `128` |
| `ObservationInbox` (`observation.rs`) | Maximum request lifetime (`MAX_LIFETIME`) / minimum directory scan interval (`SCAN_INTERVAL`) | `30 s` / `10 ms` |

## Core Invariants

* **Lower-bound continuous checkpointing (`OBSERVE_CURRENT`)**: Whereas legacy mode `0` requires an exact account waterline match, mode `2` (permitted only on approved `allowed_build_hashes` SDK builds) sets `report_sequence = floor` (`reports.last_position()`) as a minimum floor. The worker continues processing market, account, timer, and external inputs until its report position reaches `>= floor`, seals a checkpoint once, buffers the `CHECKPOINT_SEALED` reply (`opcode 40`) across `SOCK_SEQPACKET` backpressure, and discards any query already past `valid_until_mono_ns`. Periodic background `READY` announcements occupy a single replaceable slot behind explicit query replies and may advance independently even without new account events.
* **Challenge binding and double-sampled child identity**: `begin_observation` samples `before = Kernel` (`Registry::observe` or `Registry::observe_resources`), increments `seal_sequence`, and hashes `"tickwire.host.observation.v1"` over `(instance, seal_sequence, control_epoch, request_id, host_boot, pid, start_ticks)` into a 16-byte `challenge`. On completion (`finish_observation`), the host verifies `reply.ready.ready_sequence > max(last_ready_sequence, query_watermark)`, loads and validates the durable recovery closure from disk, checks that `prefixes.get(exec_report_sequence)` matches the recovered `account_input_digest` and `reports.confirms_cursor(...)` matches the live shared-memory report cursor, resamples `after = Kernel`, requires `same_process(&before, &after)` (identical identity, PID, UID/GID, kernel `boot_id`, `start_ticks`, ELF digest/size, `control_epoch`, and—if `require_resources` or a resource domain is present—cgroup device/inode and `ResourceLimits`), and confirms `completed < request.deadline <= session.deadline()`.
* **Late-reply quarantine and non-reusable request IDs**: Every accepted `request_id` is permanently recorded in `ids` (`<= 4,096`) even if dispatch later fails. If a sent query expires or is cancelled via `cancel_observation` before its reply arrives, `close()` moves the `Sent` proof into `closed` (`<= 16`), and `idle(now)` continues reporting `Error::Busy` until either the worker's matching reply arrives (updating `query_watermark` and removing the `closed` entry) or `now >= valid_until_mono_ns` guarantees the worker has dropped the expired request. Neither a past `READY`, a closed challenge, nor a restarted worker generation can satisfy a fresh query.
* **Write-ahead local inbox durability and conflict quarantine**: `ObservationInbox` authenticates local callers via `0700` directory ownership (`owner_uid`) and single-link `0600` regular files (`<32-lowercase-hex-id>.json`), rejecting symlinks, hard-link swaps, permission drift, or replacement of its private `LOCK` inode (explicitly unlocking on `Drop` so transient forked descriptors cannot extend ownership). At every startup, `open` publishes a fresh `outbox/session.json` replay boundary. Each valid request is `fsync`ed as a `Received` journal record *before* host dispatch; upon verification, exact binary recovery dependencies are content-addressed and deduplicated (verifying byte equality across duplicate roots), a `Terminal` journal record is synced, and the result JSON is atomically published in `outbox/`. Submitting identical request bytes reuses the stored outcome; submitting different bytes under an existing `request_id` cancels any pending query, syncs a `Conflict` record naming the conflicting digest, and permanently closes that ID without mutating completed historical results.

## Operations and Failure Semantics

* **Cold-only I/O and bounded saturation**: Idle polls perform zero `/proc` or recovery-closure disk reads. If rapid account reports overflow the 4,096-entry `Prefixes` ring while a query is pending, only that observation fails with `Error::ObservationCapacity` while normal report publication and strategy execution continue uninterrupted.
* **Crash recovery without re-dispatch**: On restart (`recover = true`), `ObservationInbox` verifies journal framing, digests, ordering, and referenced CAS roots, marks any incomplete `Received` request as `Interrupted` (never re-dispatching stale challenges to workers), and marks recovered completed entries with `historical = true` while preserving their original timestamps and deadlines for consumer freshness checks. `AppStrategy` routes queries strictly by `(logical_strategy_id, instance, generation)` and resamples monotonic time and `control::Binding` immediately before and after publishing any outbox file.

## Verification and References

```sh
cargo test --locked -p tickwire-strategy-host -p tickwire-app
cargo clippy --locked -p tickwire-strategy-host -p tickwire-app --all-targets -- -D warnings
```

Archived Linux process tests exercise consecutive queries on unchanged and advancing state, observed report positions exceeding the requested floor, timeouts, `SIGSTOP`/exited workers, late-reply quarantine, inbox WAL crash recovery (`Interrupted` vs. `historical`), conflicting duplicate IDs, and uninterrupted trading input flow. On the archived October Linux host, positive end-to-end cgroup resource observation remained unexecuted due to the absence of a delegated writable cgroup v2 leaf.

* Host and app inbox implementation: [observation.rs (strategy-host)](../../crates/tickwire-strategy-host/src/observation.rs), [fresh.rs](../../crates/tickwire-strategy-host/src/readiness/fresh.rs), [observation.rs (app)](../../crates/tickwire-app/src/observation.rs)
* Archived validation evidence: [runtime-observation.json](../evidence/20261029/runtime-observation.json), [verify-runtime-observation-acceptance.py](../evidence/20261029/verify-runtime-observation-acceptance.py)
* Related specifications: [Strategy parameter consumption](strategy-parameter-consumption.md), [Strategy input consumption](strategy-input-consumption.md), [Supervisor process](supervisor-process.md), [IPC wire codec](ipc-codec.md)
