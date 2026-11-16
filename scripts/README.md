# Test, Contract, and Formal Verification Guide

Tickwire validates behavior across four complementary layers: 42-crate Rust unit/integration/doc tests, multi-process Linux SHM/affinity and loopback TLS laboratories, language-independent Python contract validators, and exhaustive TLC/Apalache formal verification. Run all commands below from the workspace root.

## Core Verification Matrix

| Verification Layer | Command | What It Exercises |
| :--- | :--- | :--- |
| **Rust Workspace & Lints** | `./scripts/check-rust.sh` | Formatting (`cargo fmt`), `--locked` unit/integration/bench (`TICKWIRE_BENCH_SAMPLES=1000`) and doc tests across all 42 crates, plus `cargo clippy -- -D warnings` |
| **Linux Multi-Process IPC & CPU Affinity** | `./scripts/check-linux.sh [--native\|--docker]` | Real `memfd` seals, `SCM_RIGHTS` FD passing, `SO_PEERCRED` auth, cross-process JOIN, and `taskset -c 0` `/proc/self/task/<tid>/status` `housekeeping_cpus` CPU-isolation suites (`affinity`, `housekeeping_affinity`, `stateless_join_and_affinity`, `cpu_isolation`; native Linux by default or Docker via `--docker` / `TICKWIRE_LINUX_DOCKER=1`) |
| **Stateless 3-Process Trading Lab** | `./scripts/run-core-chain-linux.sh [spot-fill] [dev\|release]` | Stateless 3-process Binance Spot lifecycle (`tickwire-md` + `tickwire-strategy-worker` + unified ORS `tickwire-app`) over loopback TLS with inline SHM rings, DOP logon, cold REST `tw1...` bootstrap, commission fill settlement, `SIGUSR2` emergency cancel, and `SIGTERM` `OrsShutDown` (defaults to `spot-fill dev`; native Linux or Docker via `TICKWIRE_LINUX_DOCKER=1`) |
| **Shared-Feed Multi-Strategy** | `python3 scripts/run-two-strategies.py` | Single `tickwire-md` quote feed consumed by two supervised `tickwire-counting-worker` PIDs at independent `memfd` ring cursors (`--venue {bybit,okx}`, `--live`, `--proxy IP:PORT`, `--keep`) |
| **BBO Decode Benchmark** | `cargo bench --locked -p tickwire-md --bench spot_bbo_decode` | Per-frame p50 / p99 / p99.9 latency for Bybit and OKX JSON-to-integer quote normalization |
| **Order-Book Benchmark** | `cargo bench --locked -p tickwire-book --bench depth_apply` | Release-build price-level update latency across sparse and dense book workloads |
| **TLA+ & Apalache Verification** | `./scripts/check-tla.sh [family ...]` | Per-line `anchors.lock` SHA-256 zero-drift check, exhaustive TLC 2.19 state-space exploration with 100% nonzero action coverage across all `21` model families (`40` models), and Apalache `0.63.0` + Z3 symbolic `IndInv` proofs across `11` Snowcat-typed models |

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
  * **[`verify-ipc-contract.py`](verify-ipc-contract.py)** validates ABI 1.0 field widths, offsets, zeroed reserved regions, Castagnoli CRC32C checksums, inline `1024B` `IntentRecord` / `512B` `ReportRecord` / `512B` `MarketRecord` slots, DOP logon opcodes, and semantic rejection cases across `48` fixed layouts, `37` data message IDs, `38` control opcodes, `86` golden vectors, and `32` semantic cases.
  * **[`verify-risk-vectors.py`](verify-risk-vectors.py)** independently recomputes Spot and USDⓈ-M reservations, haircuts, and outward rounding in arbitrary-precision decimal math (`21` cases, `131` expected fields).
  * **[`verify-instrument-catalog.py`](verify-instrument-catalog.py)** verifies canonical catalog serialization, SHA-256 digests, and economic hashes (`3` catalog vectors, `22` rehashed rejection checks).
  * **[`verify-config-contract.py`](verify-config-contract.py)** validates `config/reference.toml`, `config/production.template.toml`, `system-config.schema.json`, scoped projections, exact reference SHM commitment (`5,910,888,448` bytes within the `8,589,934,592`-byte host SHM budget), `housekeeping_cpus` vs. hot-core isolation (`hot_cpus ∩ housekeeping_cpus == ∅`), multi-UID `enabled_accounts`, `order_manager`, and static `rms` limits.
* **[`verify-documentation.py`](verify-documentation.py)** checks English-only prose (zero CJK characters), balanced fenced code blocks, local file links, and Markdown heading anchors across all guides and specifications.

---

## TLC, Source-Anchor, and Apalache Verification Runner

`./scripts/check-tla.sh` accepts zero, one, or multiple model-family names (`./scripts/check-tla.sh [family ...]`) across the `21` model families (`40` TLC models and `11` Snowcat-typed symbolic `IndInv` models) and enforces three gates:

1. **Zero-drift source anchors (`02_models/anchors.lock`):** Verifies per-line SHA-256 digests across all `44,618` cited core-crate Rust source lines before launching any JVM (`TLA_SKIP_ANCHORS=1` to skip).
2. **Exhaustive TLC 2.19 & 100% action coverage:** Verifies the SHA-256 checksum of `tla/tools/tla2tools.jar` (`v1.7.4` release bundling `TLC 2.19`), stages each model and its `.cfg` files into an isolated temporary directory, emits zero-touch `@TLAPlusOperator` JVM bytecode overrides via [`tlc_native_overrides.py`](tlc_native_overrides.py) (accelerating 10-limb `L0_Calibration` `Word` arithmetic and memoizing `L0_DepthSync` `Events(t, g)` set enumeration without modifying frozen `.tla` sources or SHA-256 digests), schedules tasks in longest-processing-time (LPT) order across a CPU-core token pool (`TLA_JOBS` total cores, optional `-Xmx${TLA_MAX_HEAP}` override) to completion (`0` states remaining on queue), and asserts in deterministic order that every modeled action generates successors via [`tlc_coverage.py`](tlc_coverage.py). Coverage-basis configurations execute with `-coverage 60` (`-workers 1..4` scaled by state count, C1 fast-JIT below `25k` states), while multi-million-state scaling configurations (`>= 2M` states) whose sibling `.cfg`s already cover all model actions execute with multi-worker `OffHeapDiskFPSet` and congruent `VIEW` quotients (`DepthSyncView`, `CalibView`) in isolated scratch directories (falling back automatically to `-workers 1 -coverage 60` if any action were uncovered).
3. **Unbounded Apalache 0.63.0 + Z3 symbolic induction:** When Apalache (`tla/tools/apalache/lib/apalache.jar`, `APALACHE_JAR`, or `APALACHE_BIN`; `TLA_SKIP_APALACHE=1` to skip) is installed, runs Snowcat static typechecking plus `base` (`Init => IndInv`), `step` (`IndInv /\ Next => IndInv'`), and safety `entailment` checks across every Snowcat-typed `IndInv` specification.

To run unit tests for the TLC coverage parser and native override emitter:

```sh
python3 -m unittest discover -s scripts -p test_tlc_coverage.py
```
