# Test, Contract, and Formal Verification Guide

Tickwire validates behavior across four complementary layers: 42-crate Rust unit/integration/doc tests, multi-process Linux SHM/affinity and loopback TLS laboratories, language-independent Python contract validators, and exhaustive TLC/Apalache formal verification. Run all commands below from the workspace root.

## Core Verification Matrix

| Verification Layer | Command | What It Exercises |
| :--- | :--- | :--- |
| **Rust Workspace & Lints** | `./scripts/check-rust.sh` | Formatting (`cargo fmt`), `--locked` unit/integration/doc tests across all 42 crates, and `cargo clippy -- -D warnings` |
| **Linux Multi-Process IPC & CPU Affinity** | `./scripts/check-linux.sh` | Real `memfd` seals, `SCM_RIGHTS` FD passing, `SO_PEERCRED` auth, cross-process JOIN, and `/proc/self/task/<tid>/status` `housekeeping_cpus` CPU-isolation tests (native Linux or Docker via `TICKWIRE_LINUX_DOCKER=1`) |
| **Stateless 3-Process Trading Lab** | `./scripts/run-core-chain-linux.sh spot-fill release` | Stateless 3-process Binance Spot lifecycle (`tickwire-md` + `tickwire-strategy-worker` + unified ORS `tickwire-app`) over loopback TLS with inline SHM rings, DOP logon, cold REST `tw1...` bootstrap, and fee closure |
| **Shared-Feed Multi-Strategy** | `python3 scripts/run-two-strategies.py` | Single `tickwire-md` quote feed consumed by two supervised worker PIDs at independent `memfd` ring cursors |
| **BBO Decode Benchmark** | `cargo bench --locked -p tickwire-md --bench spot_bbo_decode` | Per-frame p50 / p99 / p99.9 latency for Bybit and OKX JSON-to-integer quote normalization |
| **Order-Book Benchmark** | `cargo bench --locked -p tickwire-book --bench depth_apply` | Release-build price-level update latency across sparse and dense book workloads |
| **TLA+ Model Checking** | `./scripts/check-tla.sh` | Exhaustive TLC state-space exploration and nonzero action coverage across all model families (requires Java) |

---

## Machine Contracts and Documentation Checks

Contract checks require Python 3.11+ and the `jsonschema` package pinned in `scripts/requirements-design.txt`:

```sh
python3 -m venv /tmp/tickwire-contracts
/tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
python3 scripts/verify-documentation.py
```

* **[`verify-design-contracts.py`](verify-design-contracts.py)** orchestrates all four independent machine-contract validators:
  * **[`verify-ipc-contract.py`](verify-ipc-contract.py)** validates ABI 1.0 field widths, offsets, zeroed reserved regions, Castagnoli CRC32C checksums, inline `1024B` `IntentRecord` / `512B` `ReportRecord` slots, DOP logon opcodes, and semantic rejection cases across `48` fixed layouts and `86` golden vectors.
  * **[`verify-risk-vectors.py`](verify-risk-vectors.py)** independently recomputes Spot and USDⓈ-M reservations, haircuts, and outward rounding in arbitrary-precision decimal math.
  * **[`verify-instrument-catalog.py`](verify-instrument-catalog.py)** verifies canonical catalog serialization, SHA-256 digests, and economic hashes.
  * **[`verify-config-contract.py`](verify-config-contract.py)** validates `config/reference.toml`, `config/production.template.toml`, `system-config.schema.json`, scoped projections, exact SHM capacity (`5,910,888,448` bytes), `housekeeping_cpus` vs. hot-core isolation (`hot_cpus ∩ housekeeping_cpus == ∅`), multi-UID `enabled_accounts`, `order_manager`, and static `rms` limits.
* **[`verify-documentation.py`](verify-documentation.py)** checks English-only prose (zero CJK characters), balanced fenced code blocks, local file links, and Markdown heading anchors across all guides and specifications.

---

## TLC Coverage Runner

`./scripts/check-tla.sh` verifies the SHA-256 checksum of the pinned TLC 1.7.4 distribution (`tla/tools/tla2tools.jar`), accepts zero, one, or multiple model-family names (`./scripts/check-tla.sh [family ...]`), stages each model and its `.cfg` files into an isolated temporary directory, emits zero-touch `@TLAPlusOperator` JVM bytecode overrides via [`tlc_native_overrides.py`](tlc_native_overrides.py) (accelerating 10-limb `L0_Calibration` `Word` arithmetic and memoizing `L0_DepthSync` `Events(t, g)` set enumeration without modifying frozen `.tla` sources or SHA-256 digests), schedules configurations in longest-processing-time (LPT) order across a CPU-core token pool (`TLA_JOBS` total cores, optional `-Xmx${TLA_MAX_HEAP}` override) to completion (`0` states remaining on queue), and asserts in deterministic order that every modeled action generates successors via [`tlc_coverage.py`](tlc_coverage.py). Coverage-basis configurations execute with `-workers 1 -coverage 60` (and C1 fast-JIT for small state spaces), while multi-million-state scaling configurations whose sibling `.cfg`s already cover all model actions execute with multi-worker `OffHeapDiskFPSet` and enlarged YoungGen sizing (falling back automatically to `-coverage 60` if any action were uncovered).

To run unit tests for the TLC coverage parser and native override emitter:

```sh
python3 -m unittest discover -s scripts -p test_tlc_coverage.py
```
