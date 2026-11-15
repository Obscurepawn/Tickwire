# tickwire-binance-execution

`tickwire-binance-execution` (`#![forbid(unsafe_code)]`) bridges `tickwire-account-gateway` and
`tickwire-binance-account::ws` with calibrated `SessionClock` timestamps, `tickwire-binance-quota::Endpoint`
rate-limit permits, and `tickwire-binance` request signing via `SharedSessionState<C, B>`. Single-threaded Binance
account runtimes use it to sign outbound REST and WebSocket trading commands once and re-verify the original signed
timestamp, delivery budget, and quota permit at the gateway's first-application-byte gate. Account balances, WAL
storage, and socket I/O live in adjacent crates.

## Invariants and Change Guidelines

* **Single-signature immutability, concurrent WS plans, non-freezing local refusals, and windowed eviction**: Sign each
  `CanonicalCommand` once against its sampled `SignedTime` and cache `SignedRequest` and `WsRequestPlan` by command
  `Hash` in [src/lib.rs](src/lib.rs) (bounded by `max_signed_commands`), supporting concurrent WebSocket `WsRequestPlan`
  and `DurableWsBinding` entries (`1..=16`) on the same active `SessionIdentity`; never re-sign with a newer timestamp
  if a queued request waits behind transport backpressure. When a local per-intent admission refusal occurs before
  binding commit (such as `CrossesBook`, `OrderNotOpen`, `TooManyPendingActions`, `InvalidPrice`, `InvalidQuantity`, or
  `NoChange`), `SharedSessionState::discard_uncommitted_websocket` retires the uncommitted `WsRequestPlan` and cached
  `SignedRequest` without consuming `Endpoint` quota or halting the WebSocket session. Lazily evict consumed, expired,
  or superseded entries past `retention_window_ns` (default `24 h`, configurable via
  `SharedSessionState::set_retention_window_ns`) before rejecting new commands with `Error::Capacity`.
* **First-byte clock, budget, and quota recheck**: At `Authority::current_gate` (for REST) and WebSocket first-write
  gates in [src/lib.rs](src/lib.rs), re-verify `SessionClock::check_send_sample` using `DeliveryBudget::remaining_ns`
  and `Endpoint` command permits (plus optional `MultiIpCommandQuota` via `with_multi_ip_quota`) on the exact original
  `SignedTime` before a single byte leaves the process.
* **Zero secret serialization and single-thread sharing**: `credential_reference` hashes only public metadata and
  `api_key` (`tickwire.ws.credential-reference.v1`), never signing secrets; keep `SharedSessionState` on
  `Rc<RefCell<...>>` so it cannot cross threads or mint send authority outside the owning reactor.
* **Immediate quota freeze on `418`/`429`**: Propagate HTTP or WebSocket `418`/`429` status feedback directly to
  `Endpoint::freeze()` (and `MultiIpCommandQuota::note_account_banned` on `418`) so subsequent outbound attempts halt
  before touching the socket.
* **Side-effect-free idle-window hot-path cache warming (`warmup`)**: In [src/warmup.rs](src/warmup.rs) and
  [src/lib.rs](src/lib.rs), `SharedSessionState::set_warmup_interval_ns` seeds `record_activity` from `sample_mono_ns()`
  on cold enablement (`warmup_interval_ns > 0 && last_activity_ns == 0 && last_warm_ns == 0`), and `BinanceHotPathWarmer`
  plus `SharedSessionState::maybe_warm_hot_path_with_account` (`warmup_interval_ns == 0` disabled by default) refuse to
  fire while any unconsumed `SignedCommand` awaits first-byte gate acceptance or while
  `now_mono_ns - last_activity_ns < warmup_interval_ns`, dry-running stack `bookTicker` decode, read-only `&Account` /
  `StpBook` / `Fixed` math, stack RFC 2104 HMAC-SHA256 signing + REST/RFC 6455 WS framing, and `TlsLoopbackWarmer`
  roundtrips without touching `signed`, `plans`, `Endpoint` / `MultiIpCommandQuota`, `AccountJournal`, or live TLS
  connections.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/warmup.rs](src/warmup.rs) · [src/tests.rs](src/tests.rs)
* **Specifications and models**: [binance-execution.md](../../specs/implementation/binance-execution.md) ·
  [binance-ws-trading.md](../../specs/implementation/binance-ws-trading.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-execution --all-targets
./scripts/check-tla.sh binance_roundtrip
```
