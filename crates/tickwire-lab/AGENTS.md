# `tickwire-lab` Engineering Guide (`AGENTS.md`)

## 1. Crate Scope and Loopback Fencing

`tickwire-lab` owns the explicit, loopback-only 3-process Linux verification laboratory exercising:
1. **`tickwire-md`**: Public market-data depth snapshot & WebSocket stream synchronization and sealed `memfd` shared-memory ring broadcast.
2. **`tickwire-app`**: Unified stateless single-venue ORS execution process (`1 ORS process = 1 Venue`), cold-recovering balances, open-order holds, and per-strategy sequence floors exclusively from authenticated Exchange REST snapshots (`GET /api/v3/account` + `GET /api/v3/openOrders` + 32-character `[0-9a-z]` `SelfDescribingClientId` `tw1...` tokens), authenticating `/ws-api/v3` via Ed25519 `session.logon`, and handling `SIGUSR2` (`EmergencyCancelAll`) and `SIGTERM` (`OrsShutDown` + `account-report.json`).
3. **`tickwire-strategy-worker`**: Stateless strategy worker joining `tickwire-md` over `SCM_RIGHTS`, completing the `DOP` dual-seqnum handshake (`DopSession::connect_and_login`), publishing an inline `1,024B` `IntentRecord` (`order_seqnum = 4`, advancing past cold-recovered `order_seqnum = 3`), and draining inbound `512B` `ReportRecord` replies (`Ack`, `Fill`, `OrsRestricted`, `OrsShutDown`).

Production deployments never load `tickwire-lab`. [`Scenario::validate`](src/lib.rs) rejects any non-loopback address, relative path, or `fixture_only == false` configuration.

## 2. Module Layout & Invariants

* [`src/lib.rs`](src/lib.rs): `#![forbid(unsafe_code)]` crate root and `Scenario` fencing validator.
* [`src/model.rs`](src/model.rs): Exact `Fixed` fixed-point scenario balances/fees (`0.06000000 USDT` commission), `spot_client_id_domain`, cold `tw1...` `SelfDescribingClientId`, and `CanonicalCommand` builder.
* [`src/generate.rs`](src/generate.rs): Generates single-use `0o700` scenario directories (`<= 65` byte root path for `AF_UNIX` `SOCK_SEQPACKET` sockets), ephemeral TLS trust anchors, Ed25519 + HMAC keys, `md/config.json`, `ors/config.json`, and `strategy/bootstrap.json` with zero local WAL, journal, or disk blob files.
* [`src/fixture.rs`](src/fixture.rs): Deterministic TLS Binance Spot counterpart verifying REST HMAC signatures, Ed25519 `session.logon`, and 32-character `tw1...` client IDs while recording `fixture.jsonl`.
* [`src/launch.rs`](src/launch.rs): Spawns the 3 real Linux processes + `fixture`, verifies cold recovery, live quote-to-order round-trip, `SIGUSR2` + `SIGTERM` ordered drain, and exact post-fill `Fixed` balances.
* [`src/main.rs`](src/main.rs): `tickwire-lab` CLI (`generate`, `serve`, `run`, and internal PID-preserving `child-exec`).

## 3. Verification

```sh
cargo test --locked -p tickwire-lab --all-targets
cargo clippy --locked -p tickwire-lab --all-targets -- -D warnings
./scripts/run-core-chain-linux.sh
```
