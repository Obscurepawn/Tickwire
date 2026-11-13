# tickwire-lab

`tickwire-lab` (`#![forbid(unsafe_code)]`) owns the loopback-only multi-process integration
laboratory (`tickwire-lab`), combining a local rustls/HMAC Binance exchange counterpart
([src/fixture.rs](src/fixture.rs)), ephemeral certificate and configuration generator
([src/generate.rs](src/generate.rs)), PID-gated process launcher ([src/launch.rs](src/launch.rs)),
and post-run journal/accounting verifier ([src/model.rs](src/model.rs)). Production binaries never
load this crate; real `tickwire-md`, `tickwire-app`, and strategy workers execute unmodified against
the local TLS fixture.

## Invariants and Change Guidelines

* **Loopback and single-use scenario fencing**: `Scenario::read` ([src/lib.rs](src/lib.rs)) and
  `fixture::serve` ([src/fixture.rs](src/fixture.rs)) require `schema == 1`, `fixture_only == true`,
  loopback `SocketAddr` peers, absolute paths, unexpired `valid_until_ns` in the host `ClockDomain`,
  and absence of a prior `STARTED` marker so laboratory credentials and directories cannot leak onto
  external interfaces or be replayed accidentally.
* **PID-preserving admission handshake**: In [src/launch.rs](src/launch.rs), spawn child processes
  via `child-exec` waiting on a gate file and `CommandExt::exec` into the target binary so the
  launcher registers exact kernel PIDs for MD/control admission before releasing the gate, arming
  live quote flow (`ARM`) only after observing `account_reconciled` and `strategy_reports_consumed`.
* **End-to-end economic and journal verification**: Changes to fixture event sequencing
  ([src/fixture.rs](src/fixture.rs)) or scenario generation ([src/generate.rs](src/generate.rs))
  must satisfy the independent cash, fee, reservation, and worker-report assertions in
  [src/model.rs](src/model.rs) and the Linux core-chain runner ([README.md](README.md)).

## Verification and Specifications

* **Source and guide**: [src/lib.rs](src/lib.rs) · [src/main.rs](src/main.rs) ·
  [src/fixture.rs](src/fixture.rs) · [src/generate.rs](src/generate.rs) ·
  [src/launch.rs](src/launch.rs) · [src/model.rs](src/model.rs) · [README.md](README.md)
* **Specifications and models**:
  [single-host-trading.md](../../specs/implementation/single-host-trading.md) ·
  [binance-execution.md](../../specs/implementation/binance-execution.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-lab --all-targets
./scripts/run-core-chain-linux.sh spot-fill
```
