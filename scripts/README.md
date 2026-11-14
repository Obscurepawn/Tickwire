# Test, Contract, and Formal Verification Guide

Tickwire validates behavior across four complementary layers: workspace-wide Rust unit/integration tests, multi-process Linux IPC and TLS laboratories, language-independent Python contract validators, and exhaustive TLC finite-state model checking. Run all commands below from the workspace root.

## Core Verification Matrix

| Verification Layer | Command | What It Exercises |
| :--- | :--- | :--- |
| **Rust Workspace & Lints** | `./scripts/check-rust.sh` | Formatting (`cargo fmt`), workspace unit/integration/doc tests, and `cargo clippy -- -D warnings` |
| **Linux Multi-Process IPC** | `./scripts/check-linux.sh` | Real `memfd` seals, `SCM_RIGHTS` FD passing, `SO_PEERCRED` auth, and cross-process JOIN (requires Docker) |
| **End-to-End Binance Lab** | `./scripts/run-core-chain-linux.sh spot-fill release` | Full Binance Spot order lifecycle over loopback TLS, WAL persistence, worker reports, and fee closure |
| **Shared-Feed Multi-Strategy** | `python3 scripts/run-two-strategies.py` | Single public quote feed consumed by two supervised worker PIDs with independent cursors and checkpoints |
| **BBO Decode Benchmark** | `cargo bench --locked -p tickwire-md --bench spot_bbo_decode` | Per-frame p50 / p99 / p99.9 latency for Bybit and OKX JSON-to-integer quote normalization |
| **Order-Book Benchmark** | `cargo bench -p tickwire-book --bench depth_apply` | Release-build price-level update latency across sparse and dense book workloads |
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

* **[`verify-design-contracts.py`](verify-design-contracts.py)** orchestrates all contract validators and checks WAL v1 header/trailer geometry and canonical venue-event byte vectors:
  * **[`verify-ipc-contract.py`](verify-ipc-contract.py)** validates ABI 1.0 field widths, offsets, zeroed reserved regions, CRC32C checksums, and semantic rejection cases.
  * **[`verify-risk-vectors.py`](verify-risk-vectors.py)** independently recomputes Spot and USDⓈ-M reservations, haircuts, and outward rounding in arbitrary-precision decimal math.
  * **[`verify-instrument-catalog.py`](verify-instrument-catalog.py)** verifies canonical catalog serialization, SHA-256 digests, and economic hashes.
  * **[`verify-config-contract.py`](verify-config-contract.py)** validates `config/reference.toml`, scoped projections, JSON schemas, and cross-field rejection cases.
* **[`verify-documentation.py`](verify-documentation.py)** checks English-only prose, fenced code blocks, local file links, and Markdown heading anchors across all guides and specifications.

---

## TLC Coverage Runner

`./scripts/check-tla.sh` verifies the SHA-256 checksum of the pinned TLC 1.7.4 distribution (`tla/tools/tla2tools.jar`), accepts zero, one, or multiple model-family names (`./scripts/check-tla.sh [family ...]`), stages each model and its `.cfg` files into an isolated temporary directory, executes single-worker (`-workers 1 -coverage 1`) configurations concurrently across a bounded LPT-scheduled worker pool (`TLA_JOBS` workers, `-Xmx${TLA_MAX_HEAP:-2560m}` per JVM) to completion (`0` states remaining on queue), and asserts in deterministic order that every modeled action generates successors via [`tlc_coverage.py`](tlc_coverage.py).

To run unit tests for the TLC coverage parser itself:

```sh
python3 -m unittest discover -s scripts -p test_tlc_coverage.py
```
