# Stateless Market-Data Publisher Isolation and Live Topic Expansion

`tickwire-md` runs as a pure stateless shared-memory market-data publisher with optional hot `reactor_cpu` and cold `housekeeping_cpus` (`md-cold`) thread affinity isolation and zero local persistence. On Binance multi-feed deployments, a running `tickwire-md` process can also append pre-approved catalog instruments to an active shared-memory market-data topic in memory without restarting existing venue sockets or disturbing unexpanded topics. Existing instruments retain their dense local slots, a successor shared-memory bank publishes the enlarged instrument set, and every approved consumer executes a coordinated quiesce-and-rejoin transition.

This protocol governs market-data topic membership, CPU isolation, and replica handoff exclusively. Account assets, cash balances, order-entry routes, and trading permissions maintain independent lifecycles inside the ORS execution process, and the single-instrument Bybit/OKX spot BBO service (`spot_market`) runs the same hot/cold CPU isolation contract without multi-instrument topic expansion.

## State Model, Affinity Isolation, and Bounds

`tickwire-md` loads immutable static startup configuration and optional CPU affinity masks (`reactor_cpu: Option<u32>` and `housekeeping_cpus: Option<CpuSet>`, both defaulting to `None` for single-core and container environments). At startup, `tickwire_control_sys::validate_cpu_isolation` enforces `reactor_cpu ∩ housekeeping_cpus == ∅` and rejects out-of-range CPU IDs (`>= 4096`) before binding sockets or allocating shared memory. The background cold worker thread (`md-cold`) is spawned via `tickwire_control_sys::spawn_pinned_thread("md-cold", housekeeping_cpus.as_ref(), ...)` *before* running cold DNS/TLS/HTTPS snapshot or topic-expansion tasks and *before* pinning the main reactor thread to `reactor_cpu` via `tickwire_control_sys::pin_current_thread`, allowing `md-cold` to escape an external single-core `taskset` process mask onto `housekeeping_cpus`.

Public HTTP snapshot and listen-key weight tracking runs through an in-memory `PublicQuota` store (`tickwire-quota`) with zero disk files. When live topic expansion is enabled, an operator stages a schema-`1` JSON `Plan` inside the configured private control directory, binding the expected in-memory base state to the target configuration and warmup budgets:

| Resource or Parameter | Bound | Enforcement Rule |
| --- | --- | --- |
| `reactor_cpu` & `housekeeping_cpus` | CPU IDs `< 4096` | Optional (`None` default); `reactor_cpu ∩ housekeeping_cpus == ∅` verified via `sched_getaffinity` read-back on pinned threads. |
| `Plan` payload | `<= 4 MiB` (`MAX_PLAN_BYTES`) | Nonzero 16-byte `operation_id`; canonical digest ignores JSON whitespace. |
| Operation history | `128` operation IDs | Bounded in-memory table retains canonical digests (including rejected plans); reusing an ID with different bytes fails. |
| Warmup & freshness budgets | `<= 60 s` (`MAX_WARMUP_NS`) | `now < warmup_deadline_mono_ns` and `0 < maximum_age_ns <= 60 s`. |
| Feed & admission fanout | `<= 32` feeds in `Plan`, `<= 128` server participants | Target feeds must be a strict superset of current feeds (`<= 3` event streams/feed). |
| Peak memory budgets | `old_cache + new_cache <= peak_cache_bytes` | Shared-memory budget covers `retained + old_bank + new_bank <= maximum_retained_bank_bytes`. |

The target configuration must leave every existing instrument's economics, catalog hash/version, source bindings, book keys, connection settings, directory identities, and sibling topics bit-for-bit unchanged. Only appended feeds, `target_revision = base_revision + 1`, `instrument_set_version` (`< 2^63`), and non-decreasing topic cache/snapshot capacities may advance. The successor bank template preserves producer identity, channel, catalog, ring geometry, and `host_boot_id`, advances `ring_epoch` and `feed_generation`, and initializes `created_mono_ns = 0`, `cutover_seq = 0`, `first_seq = 1`, and an unset `descriptor_hash` (unless supplied matching its exact wire bytes) to be finalized at cutover.

## Core Invariants

* **Disjoint hot reactor vs. `md-cold` housekeeping execution**: DNS resolution, TLS client construction, HTTPS depth snapshot fetches, candidate warmup preparation, and cold plan I/O execute on `md-cold` (`housekeeping_cpus`), while the single `tickwire-md` reactor thread (`reactor_cpu`) retains in-memory quota admission, nonblocking `mio::Poll` socket drains, book updates, and SHM ring publication.
* **Slot and live-socket preservation**: `FeedIndex::expanded` clones a private copy of the current mapping, keeping every pre-existing instrument at its original `DenseId` and appending new instruments in first-seen order. Active upstream depth and event sockets retain their network tokens uninterrupted, while retired candidate tokens and 16-byte `owner_id` values are permanently burned and never silently reused.
* **Reversible pre-revocation, fail-closed post-revocation**: Any validation, warmup, budget, or candidate write failure *before* old-bank revocation aborts cleanly and leaves the existing bank and publisher running. Once old-bank revocation begins (or if bank freeze fails), any error invokes the fault sink and keeps the topic closed rather than serving split state.
* **Evidence-backed quiescence**: The old bank cannot be cut over on timeouts, broken control sockets, incomplete handshakes, or unverified text messages; every approved participant must supply mapped-cursor `QUIESCED` evidence and a detached registration.
* **Stateless cold restart**: Cold boot and process restart allocate fresh producer instances, fresh shared-memory banks starting at sequence `1`, fresh upstream WebSocket/HTTPS sessions, and fresh subscriber `JOIN` admissions from immutable startup configuration alone—never resuming retired rings or reading local recovery files.

## Prepare, Warm, and Cut Over

1. **Prepare and warm on `md-cold` and candidate owners**: `md-cold` prepares DNS resolution, TLS root stores, and client structures off the reactor thread. New Binance depth and event owners warm real HTTPS snapshots and WebSocket streams inside a private candidate publisher (`prepare_expansion`) with prebound topic slots. Warmup succeeds only when every required depth book and configured event category on the newly added instruments is continuous, usable, and within `maximum_age_ns`; any sequence gap, stale observation, disconnect, or book rebuild clears readiness immediately.
2. **Arm participants**: Bind each participant's current client admission, successor template, plan approval digest, target revision, monotonic clock, and deadline over authenticated control sessions (`BoundedRequestStore`). Unknown or unfinished participant handshakes block confirmation.
3. **Stop new JOINs and quiesce**: Once **all** current participants return `Armed` receipts, stop admitting new JOINs to the target topic and send `QUIESCE` packets carrying operation- and subscription-bound request IDs (preserving exact packet bytes across nonblocking send retries). Wait until every peer proves mapped-cursor `QUIESCED` state and detaches its active registration.
4. **Finalize and freeze at `cutover_seq`**: Copy current live state for pre-existing rows into the candidate, re-verify readiness, freshness, snapshot size, and peak cache/bank budgets, and sample `cutover_seq = old.published`. Finalize the successor `ResourceHeader` with `created_mono_ns = now`, `cutover_seq`, `first_seq = cutover_seq + 1`, `predecessor_ring_epoch = old.ring_epoch`, and its SHA-256 `descriptor_hash`. Write the successor bank, republish all member states, and freeze the predecessor bank while retaining its mappings for in-memory retirement accounting.
5. **Rejoin and confirm activation**: Every consumer performs a [JOIN](join.md) against the finalized successor descriptor, maps the enlarged state, and drains catch-up events at its independent cursor. Mark the expansion complete only after `tickwire-md`'s effective in-memory revision/digest and all live `Activated` receipts agree.

## Verification and References

```sh
cargo test --locked -p tickwire-market-publisher -p tickwire-md --all-targets
cargo clippy --locked -p tickwire-market-publisher -p tickwire-md --all-targets -- -D warnings
```

Integration and unit suites verify cold plan validation, candidate warmup and cutover, `reactor_cpu` vs. `housekeeping_cpus` conflict rejection, and `md-cold` escaping a single-core parent thread affinity mask (`Cpus_allowed_list` read-back under `/proc/self/task/<tid>/status`).

* [Cold plan validation and bank finalization](../../crates/tickwire-md/src/expansion.rs)
* [Market-data runtime and `md-cold` worker](../../crates/tickwire-md/src/engine.rs)
* [Market-data runtime expansion coordinator](../../crates/tickwire-md/src/engine/expansion.rs)
* [Topic publisher candidate warmup and cutover](../../crates/tickwire-market-publisher/src/expansion.rs)
* [Consumer market transition state machine](../../crates/tickwire-market-source/src/transition.rs)
* [Stateless JOIN and CPU affinity tests](../../crates/tickwire-md/tests/stateless_join_and_affinity.rs)
* [JOIN protocol specification](join.md)
* [Archived Linux multi-process expansion evidence (`20261028`)](../evidence/20261028/market-expansion-runtime.json)
