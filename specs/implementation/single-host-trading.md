# Stateless Single-Host 3-Process Trading Runtime (`tickwire-runtime`)

## Topology Overview

Tickwire executes live trading on a single Linux host as three cooperating, pure stateless shared-memory (`memfd`-sealed) processes with **zero local WAL, journal, blob-store, owner-lease, or dynamic config-patch files**:

1. **Market-Data Publisher (`tickwire-md`):** Sole owner of public exchange market-data feeds, sequence continuity, order-book maintenance, and SHM broadcast rings (`MarketRecord` / `MarketExactRecord`).
2. **Strategy Workers (`tickwire-strategy-sdk` / `tickwire-strategy-host`):** Join `tickwire-md` SHM broadcast rings, complete DOP logon (`DopLogin` -> `DopEcho` + `OrsSessions`) over a `0o700` Unix control socket, publish inline 1,024-byte `IntentRecord`s, and consume inline 512-byte `ReportRecord`s.
3. **Unified Venue-Agnostic ORS Process ([`tickwire-runtime`](../../crates/tickwire-runtime/src/lib.rs)):** `1 ORS process = 1 Venue`, managing `1..N` `AccountSession`s (`1 AccountSession = 1 Exchange UID`, `account_ref = 1..=255` with `0` routing to the strategy's first authorized account), holding **zero public market-data connections and zero SHM quote subscriptions**.

[`tickwire-runtime`](../../crates/tickwire-runtime/src/lib.rs) compiles under `#![forbid(unsafe_code)]`, performs zero floating-point arithmetic, and depends on **zero venue-specific crates**—all exchange protocol framing and REST payloads plug into the generic `VenueSession` trait ([`session.rs`](../../crates/tickwire-runtime/src/session.rs)).

## Reproduce Native Quote Decoding

In the stateless 3-process architecture, public quote decoding and book reconstruction execute exclusively inside `tickwire-md` and strategy workers—never inside `tickwire-runtime`. When strategy workers decode a quote from `tickwire-md`, they stamp `IntentBody::tick_mono_ns` with the triggering quote's local monotonic receipt timestamp (`CLOCK_MONOTONIC_RAW` / monotonic clock). To verify native quote decoding in isolation, run the market-data publisher and strategy SDK suites (`cargo test --locked -p tickwire-md -p tickwire-strategy-sdk`); to verify ORS stale-quote enforcement against `tick_mono_ns`, run `cargo test --locked -p tickwire-runtime --all-targets`.

## Venue-Agnostic `VenueSession` Trait (`session.rs`)

Each `AccountSession<S: VenueSession>` pairs generic ORS state (`AccountSpec`, `PublishedRoutes`, `UrlConnections` pools, `OrsAccountQuota`, `OrsQueryDesk`, `AsyncRestQuerier`, `GuardedValue` stream token) with a venue driver `S`:

- **Outbound Order Hooks (`REQ` thread):**
  - `submit_new`, `submit_cancel`, `submit_modify` (backed by default generic orchestration over `encode_new_order`, `encode_cancel_order`, `encode_modify_order`, and `build_order_spends`):
    1. Runs `OrderManager` reservation + lock-free atomic `OrsStpBook` self-cross check (`Order` spinlock acquired and released before touching any connection).
    2. Honors `dry_run` (`IntentBody::intent_flags & DRY_RUN != 0`): uses `OrderManager`'s scratch slot, immediately releases the reservation, and skips route selection, rate-limiter spend, and socket write.
    3. Selects a ready route via `PublishedRoutes::pick` respecting `OrderRoute.product_mask`, then executes `SharedRateLimiter::pick_and_spend_or_nack` on the picked connection's `local_ip`—**NACKing immediately (`RefusalReason::RateLimited`) without retrying another connection** if either account or IP budget refuses.
    4. Encodes the venue wire frame into caller-supplied stack/thread scratch storage and calls `LockedConnection::send_frame` without holding any `Order` spinlock.
- **Optional RFQ Quote Hooks:**
  - `has_quote` (`false`), `open_quotes` (`0`), `submit_quote`, and `cancel_quotes`, defaulting to immediate refusal with `RefusalReason::RfqQuoteNotSupported`.
- **Inbound & Session Lifecycle Hooks (`RESP` & `Dialer`):**
  - `connection_url`, `encode_login`, `awaits_login_reply`, `login_reply`, `login_challenge`, `encode_subscribe`, `fix_session`, `fix_logon`, `is_pong`, `reply_to_ping`, `reply_id`, `encode_ping`, `on_message`, and `on_disconnected`.
  - `SessionMessageEffects` carries `OrsVenueEvent`s, rising-edge `ExchRateLimitEvent`s, `query_status` requests (`OrsQueryDesk`), `stream_token_expired` notifications, and `graceful_drain_connection: bool` for OKX/Bybit server-maintenance `notice` smooth switchover (unpublishing the draining slot from `PublishedRoutes`, requesting a fresh replacement connection from `Dialer`, and closing the draining socket once in-flight requests reach zero).
- **Off-Hot-Path REST Hooks (`AsyncRestQuerier` & Cold Bootstrap):**
  - `has_stream_token`, `stream_token_query`, `on_stream_token_reply`, `open_orders_queries`, `order_status_query`, `status_query_delay_ms`, `apply_status`, and cold-start `bootstrap_snapshots`.

## `OrderRoutingSystem<S>` and Thread / CPU Isolation (`ors.rs`, `node.rs`)

`OrsProcess<S>` ([`node.rs`](../../crates/tickwire-runtime/src/node.rs)) enforces strict hot-vs-cold core isolation validated at startup via `tickwire_control_sys::validate_cpu_isolation(&[("req_cpu", req_cpu), ("resp_cpu", resp_cpu)], housekeeping_cpus.as_ref())`:

1. **`REQ` Hot Thread (`req_cpu`, busy-spin, zero `sleep`):**
   - Polls strategy `IntentReader`s (`1,024B` `IntentRecord`s).
   - Enforces the **microsecond stale-order gate**: when `max_order_age_ns > 0` (default `23_000` ns = `23 us`) and `intent.tick_mono_ns > 0`, if `now_mono_ns.saturating_sub(intent.tick_mono_ns) > max_order_age_ns`, refuses that single order immediately with `RefusalReason::RejectStaleOrder` (`Nack` `ReportRecord`) **without freezing any account or strategy**.
   - Checks [`OrsRuntimePolicy`](runtime-policy.md), flat lock-free `RmsEngine`, `OrderManager` (`account-gateway` / [`command-binding.md`](command-binding.md)), `PublishedRoutes`, and `SharedRateLimiter`.
   - Drains the lock-free `control -> REQ` `AdminCommands` queue when idle (`CancelOne`, `CancelAll`, `Suspend`, `Resume`, `SetStratEnabled`, `SetActive`, `LogoutClient`, `RmsReload`).
2. **`RESP` Hot Thread (`resp_cpu`, `mio::Poll` with configurable `epoll_wait_ms`, default `0`):**
   - Harvests ready connections from `Dialer` (`DialBaton`), drives `LockedConnection` socket reads/heartbeats, applies `VenueSession::on_message` / `on_disconnected` to `OrderManager` and `RmsReloadHandle::record_execution_fee`, drains `AsyncRestQuerier` replies (`OrsQueryDesk`), drains `control -> RESP` `AdminCommands` (`QueryOpen`, `Dump`, `ForceClose`, `ResendHeartbeat`), and publishes 512-byte `ReportRecord`s (`Ack`, `FrontAck`, `Nack`, `CancelNack`, `Fill`, `Canceled`, `Modified`, `ModifyRejected`, `ExchRateLimit`, `OrsRestricted`, `OrsShutDown`).
3. **Cold Housekeeping Threads (`housekeeping_cpus`):**
   - Every non-hot thread—`Dialer` (`tw-dialer`), per-account `AsyncRestQuerier` (`tw-rest-query`), `LatencyTracer` sink (`tw-lat-trace`), DOP `ControlServer` (`tw-ors-ctrl`), and the `main` `SignalWaiter` loop—is explicitly pinned to `housekeeping_cpus` via `spawn_pinned_thread` / `pin_current_thread` so inheriting a parent `taskset` never leaks background activity onto `req_cpu` or `resp_cpu`.

## DOP `ControlServer`, `AdminCommands`, and OS Signal Lifecycle (`admin.rs`, `files.rs`)

- **DOP Logon & Control (`admin.rs`):** Authenticates peers on the Unix-domain control socket, handles `DopLogin` (`effective_seq = max(login_seq, next_order_seqnum)` + `last_recv_seqnum = 0` -> `DopEcho` + `OrsSessions`), `DopLogout` (enqueuing `CancelAll` for that `strat_id`), idle `DopHeartBeat`, and operator commands (`set_strat_enabled(account_ref, strat_id, enabled)` auto-canceling open orders when `enabled == false`).
- **OS Signals & Graceful Shutdown (`node.rs`, `files.rs`):** Before spawning any worker thread, `OrsProcess` masks `SIGUSR1`, `SIGUSR2`, `SIGINT`, and `SIGTERM` via `block_ors_signals_current_thread()` and polls `SignalWaiter` on `housekeeping_cpus`:
  - `SIGUSR1`: Reads static RMS configuration via [`files.rs`](../../crates/tickwire-runtime/src/files.rs) off the hot path and calls `RmsReloadHandle::reload_limits` (retaining previous active `RmsEngine` limits on any I/O or parse error!) plus posts `QueryOpen` + `Dump` to `RESP`.
  - `SIGUSR2`: Posts emergency `CancelAll(ALL_STRATS)` to `REQ` and transitions policy to `Mode::ManageOnly` (`OrsRestricted`).
  - `SIGINT` / `SIGTERM`: Stops and joins `REQ` -> dispatches cancels for all open orders and RFQ quotes and waits up to `2s` (`shutdown_drain_timeout`) for open order and quote counts to reach `0` -> broadcasts `OrsShutDown` `ReportRecord` (`ALL_STRATS`) to every attached strategy ring -> stops `ControlServer` -> stops and joins `RESP`, `Dialer`, `AsyncRestQuerier`s, and `LatencyTracer`.

## Acceptance

1. `req_cpu == resp_cpu` and `hot_cpus ∩ housekeeping_cpus != ∅` fail fast before spawning threads; `None` defaults run cleanly on single-core CI; and when spawned from a parent thread pinned to a single hot CPU (`taskset` simulation) with `housekeeping_cpus = Some(hk)`, kernel `/proc/self/task/<tid>/status` (`Cpus_allowed_list`) reports `hk` for `ControlServer`, `Dialer`, `AsyncRestQuerier`, and `LatencyTracer`.
2. Spinlock hierarchy holds unconditionally: `Order` and `Connection` locks never overlap; `StpBook` is locked only beneath a single `Order` lock; zero logging, heap allocation, or protocol parsing occurs under any spinlock.
3. Verified by `cargo test --locked -p tickwire-runtime --all-targets` (`tests/cpu_isolation.rs` and `tests/ors_runtime.rs`).
