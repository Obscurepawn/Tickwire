# Sharing a Public Spot Quote Feed Across Strategies

In Tickwire's single-host architecture, one `tickwire-md` process terminates the public WebSocket stream (`orderbook.1` on Bybit V5 or `bbo-tbt` on OKX V5) and publishes normalized top-of-book updates into a single `memfd`-sealed shared-memory topic. Multiple strategy processes attach via authenticated JOIN handshakes and advance independent cursors—eliminating duplicate venue connections and redundant JSON parsing.

---

## 1. Run the Publisher and Two Strategy Workers Together

From the workspace root on Linux (with the pinned Rust toolchain, Python 3.11+, OpenSSL, and `strip` from GNU `binutils`), run the end-to-end launcher:

```sh
python3 scripts/run-two-strategies.py --keep
```

### What the Harness Executes and Verifies

1. Builds `tickwire-md` and the registered counting worker binary (`tickwire-strategy-example`), provisions an isolated temporary namespace, and starts a local TLS WebSocket server emitting deterministic Bybit V5 frames.
2. Spawns the market-data publisher and two supervised worker processes via `tickwire-app`'s multi-strategy host.
3. Publishes quote sequence `1000000` (`100.0` / `100.1` USDT per BTC, normalized to `1000` and `1001` integer ticks) once both workers complete their JOIN catch-up.
4. Verifies in `result.json` that two distinct worker PIDs observed sequence `1000000` in their independent checkpoints (`w1/commands/merged-inputs/HEAD.json` and `w2/commands/merged-inputs/HEAD.json`). Order emission is disabled in this observation-only profile, so no private execution connection is opened.

Because `--keep` is passed, the launcher leaves its working directory on disk (`market.json`, `control/spot-market.active`, `strategies.json`, and per-worker `w1/` and `w2/` directories) for inspection. Omit `--keep` to clean up automatically on exit.

### Switch Venues or Connect to the Live Public Exchange

```sh
python3 scripts/run-two-strategies.py --venue okx
python3 scripts/run-two-strategies.py --venue okx --live
```

With `--live`, `rustls` validates the public exchange certificate against system trust roots (pass `--proxy IP:PORT` if outbound traffic traverses an HTTP `CONNECT` proxy). Each worker records the live sequence reached at its own checkpoint cut.

---

## 2. Launch a Standalone Market-Data Owner

To run the shared-memory publisher independently and inspect its exported descriptor, choose a fresh directory with a short path (fitting within Unix domain socket `sun_path` limits):

```sh
cargo build --locked -p tickwire-md
python3 crates/tickwire-md/examples/prepare-spot-market.py bybit /tmp/tw-bybit-demo --binary target/debug/tickwire-md
target/debug/tickwire-md --spot-market /tmp/tw-bybit-demo/market.json
```

Pass `okx` (and a separate directory) to subscribe to `BTC-USDT` on OKX, or append `--proxy 127.0.0.1:7890` when behind a local proxy. Once started, the process logs `spot_market_started` and writes `control/spot-market.active`. Note that a newly exported header indicates an initialized ring, not a live quote; strategies must still check `MarketView::best_quote()` alongside feed liveness and age bounds.

---

## 3. Connect Strategy Groups and Handle Restarts

* **Attaching workers:** Configure each worker's `Bootstrap.external_market` with the publisher's Unix socket path, exact `.active` resource header, catalog digest, and instrument-set version, assigning each worker a unique instance ID, cursor slot, and JOIN request ID (see the [client template](exec-client.json)).
* **Multi-strategy account host:** A single `tickwire-app` process can host multiple strategy slots sharing one risk domain against the external market topic (see [`shared-market-strategies.json`](../../tickwire-app/examples/shared-market-strategies.json)). Run `tickwire-app inspect --config ...` prior to launch to validate slot bindings, risk domains, and generations.
* **Publisher restarts:** Confirm the previous `tickwire-md` process has exited before removing a stale Unix socket, or bind a new socket path within the control directory. Because the control ledger rejects reuse of retired headers, restarting a publisher requires incrementing both the ring epoch and feed generation in the approved `ResourceSpec`, after which consumers JOIN the successor bank.
