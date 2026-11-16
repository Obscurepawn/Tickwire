# `tickwire-lab` — Stateless 3-Process Linux Loopback Laboratory

`tickwire-lab` exercises Tickwire's complete stateless 3-process Linux trading topology end-to-end against a deterministic loopback TLS exchange fixture:

1. **`tickwire-md`**: Synchronizes a TLS Binance Spot order book (`/api/v3/depth` + `/ws/...`) and broadcasts normalized market records into a `F_SEAL_FUTURE_WRITE`-sealed `memfd` shared-memory ring.
2. **`tickwire-app`**: Cold-recovers balances, resting open-order holds, and per-strategy `DOP` sequence floors (`order_seqnum = 3 -> 4`) from authenticated Exchange REST snapshots (`GET /api/v3/account` + `GET /api/v3/openOrders` + `tw1...` `SelfDescribingClientId`), authenticates `/ws-api/v3` via Ed25519 `session.logon`, routes outbound orders, settles commission-bearing fills, cancels the cold-recovered resting order on `SIGUSR2`, and drains cleanly on `SIGTERM` with zero local write-ahead logs or disk blob files.
3. **`tickwire-strategy-worker`**: Joins `tickwire-md` over `SCM_RIGHTS`, completes `DOP` login on `ors/dop.sock`, writes an inline `1,024B` `IntentRecord` upon observing the `60,000.00` `BTCUSDT` bid, consumes `512B` `ReportRecord` replies, and exits cleanly on `OrsShutDown`.

## Running End-to-End on Linux

Run the automated core-chain script from the workspace root (runs natively on Linux by default, or inside Docker when `TICKWIRE_LINUX_DOCKER=1`):

```sh
./scripts/run-core-chain-linux.sh
```

Or invoke `tickwire-lab` manually with a short (`<= 65` bytes) single-use output directory:

```sh
cargo build --locked -p tickwire-lab -p tickwire-app -p tickwire-md \
  -p tickwire-strategy-sdk --bins
mkdir -p /tmp/tw-lab-bin
for name in tickwire-lab tickwire-app tickwire-md tickwire-strategy-worker; do
  install -m 0755 "target/debug/$name" "/tmp/tw-lab-bin/$name"
  strip --strip-debug "/tmp/tw-lab-bin/$name"
done
/tmp/tw-lab-bin/tickwire-lab generate --scenario spot-fill --out /tmp/tw-lab-run --bin-dir /tmp/tw-lab-bin
/tmp/tw-lab-bin/tickwire-lab run --config /tmp/tw-lab-run/scenario.json
```

## Subcommands and `Scenario` JSON Fencing

* `tickwire-lab generate [--scenario spot-fill] --out DIR --bin-dir DIR`: Creates a single-use `0o700` scenario directory (`<= 65`-byte absolute path), ephemeral TLS trust anchors, Ed25519 + HMAC keys, `md/config.json`, `ors/config.json`, `strategy/bootstrap.json`, and `scenario.json`.
* `tickwire-lab serve --config PATH`: Runs the standalone loopback TLS Binance Spot fixture against `scenario.json`.
* `tickwire-lab run --config PATH`: Orchestrates the 3-process run (`tickwire-md`, `tickwire-app`, `tickwire-strategy-worker`) plus `serve`, using internal `child-exec` to preserve the worker's OS PID across `tickwire-md` admission registration.
* `Scenario` (`schema = 2`, `<= 64 KiB`, `#[serde(deny_unknown_fields)]`): Enforces `fixture_only == true`, loopback `listen` IP, absolute `root` and `bin_directory` paths, non-zero `exchange_uid` and `strat_id`, and `cold_open_order_seqnum = 3`.
