# Sharing a Public Spot Quote Feed Across Strategies

In Tickwire's stateless single-host architecture, one `tickwire-md` process terminates the public WebSocket stream (`orderbook.1` on Bybit V5 or `bbo-tbt` on OKX V5) and publishes normalized top-of-book updates into a single `memfd`-sealed shared-memory topic. Multiple strategy worker processes attach via authenticated JOIN handshakes and advance independent read-only cursors—eliminating duplicate venue connections and redundant JSON parsing without writing any local WAL or disk journal state.

---

## 1. Run the Publisher and Two Strategy Workers Together

From the workspace root on Linux (with the pinned Rust toolchain, Python 3.11+, OpenSSL, and `strip` from GNU `binutils`), run the end-to-end launcher:

```sh
python3 scripts/run-two-strategies.py --keep
```

### What the Harness Executes and Verifies

1. Builds `tickwire-md` and the registered counting worker binary (`tickwire-strategy-example`), provisions an isolated temporary namespace, and starts a local TLS WebSocket server emitting deterministic Bybit V5 frames.
2. Spawns the market-data publisher and two strategy worker processes attached to the shared topic ring.
3. Publishes quote sequence `1000000` (`100.0` / `100.1` USDT per BTC, normalized to `1000` and `1001` integer ticks) once both workers complete their JOIN catch-up.
4. Verifies in `result.json` that two distinct worker PIDs observed sequence `1000000` at independent shared-memory reader cursors. Order emission can remain disabled in an observation-only profile, or exchange inline `IntentRecord` (`1024B`) and `ReportRecord` (`512B`) slots over SHM with the ORS process.

Because `--keep` is passed, the launcher leaves its working directory on disk (`market.json`, `resource.json.active`, and per-worker configuration files) for inspection. Omit `--keep` to clean up automatically on exit.

### Switch Venues or Connect to the Live Public Exchange

```sh
python3 scripts/run-two-strategies.py --venue okx
python3 scripts/run-two-strategies.py --venue okx --live
```

With `--live`, `rustls` validates the public exchange certificate against system trust roots (pass `--proxy IP:PORT` if outbound traffic traverses an HTTP `CONNECT` proxy).

---

## 2. Launch a Standalone Market-Data Owner

To run the shared-memory publisher independently and inspect its exported descriptor, choose a fresh directory with a short path (fitting within Unix domain socket `sun_path` limits):

```sh
cargo build --locked -p tickwire-md
python3 crates/tickwire-md/examples/prepare-spot-market.py bybit /tmp/tw-bybit-demo --binary target/debug/tickwire-md
target/debug/tickwire-md --spot-market /tmp/tw-bybit-demo/market.json
```

Pass `okx` (and a separate directory) to subscribe to `BTC-USDT` on OKX, or append `--proxy 127.0.0.1:7890` when behind a local proxy. Once started, the process logs `spot_market_started` and writes `resource.json.active` alongside `resource.json`. Note that a newly exported header indicates an initialized ring, not a live quote; strategies must still check `MarketView::best_quote()` alongside feed liveness and age bounds.

---

## 3. Connect Strategy Workers and Handle Restarts

* **Attaching workers:** Configure each worker's `Bootstrap.market` with the publisher's Unix socket path, exact `.active` resource header, catalog digest, and instrument-set version, assigning each worker a unique instance ID, cursor slot, and JOIN request ID (see the [client template](exec-client.json)).
* **CPU affinity isolation:** Optionally set `reactor_cpu` and disjoint `housekeeping_cpus` in `market.json` so the hot publisher loop stays isolated from the `md-cold` background thread (`tickwire-control-sys`).
* **Publisher restarts:** Confirm the previous `tickwire-md` process has exited before removing a stale Unix socket, or bind a new socket path. Restarting a publisher requires incrementing both `ring_epoch` and `feed_generation` in the approved `ResourceSpec`, after which consumers JOIN the new bank cleanly in memory.
