# Contributing to Tickwire

Tickwire is engineered as a high-assurance, stateless trading runtime where single-writer state ownership, thread/CPU isolation, fine-grained spinlock hierarchy, and exact fixed-point bounds are treated as non-negotiable invariants. This guide explains how the 42-crate repository is organized, how to prepare a Linux development environment, and how to validate changes across Rust code, machine-readable contracts, and formal TLA+ models.

## Development Environment

Tickwire targets **Linux** in production and process-level integration tests. Key subsystems rely on Linux kernel primitives encapsulated behind audited safe boundaries:

* **`memfd_create` and file sealing** (`F_ADD_SEALS`) for immutable market snapshots, single-writer shared-memory broadcast/intent/report rings, and sealed worker ELF images.
* **Unix domain sockets** with `SCM_RIGHTS` descriptor passing and `SO_PEERCRED` peer credential verification across JOIN and DOP control sessions.
* **`CLOCK_BOOTTIME`** for suspend-aware monotonic time domains (`tickwire-clock-sys`).
* **Thread CPU affinity, cgroup v2, and NIC binding** via `sched_setaffinity` + mandatory `sched_getaffinity` read-back verification (`hot_cpus ∩ housekeeping_cpus == ∅`), cgroup v2 memory/CPU limits (`tickwire-control-sys`), and `SO_BINDTODEVICE` multi-IP socket routing (`tickwire-transport`).

### Required Tools

1. **Rust toolchain:** Installed automatically via `rust-toolchain.toml` (Rust 2024 edition). Always pass `--locked` to Cargo commands so builds and tests use the exact dependency graph in `Cargo.lock`.
2. **System utilities:** OpenSSL headers/libraries and GNU `binutils` (`strip`, used by Linux process harnesses when preparing worker binaries).
3. **Python 3.11+:** Used for documentation link checking, golden-vector and JSON Schema verification (`jsonschema` in `scripts/requirements-design.txt`), and multi-process demonstration launchers.
4. **Optional verification tools:**
   * **Docker:** Optional isolated container mode (`TICKWIRE_LINUX_DOCKER=1` or `--docker`) for `./scripts/check-linux.sh` and `./scripts/run-core-chain-linux.sh` (both default to native execution on Linux hosts).
   * **Java (JRE 11+) / Apalache 0.63.0:** Required for `./scripts/check-tla.sh` (executes `anchors.lock` SHA-256 verification, the pinned TLC 2.19 model checker in `tla/tools/tla2tools.jar` [`v1.7.4` release], and—when `tla/tools/apalache/lib/apalache.jar`, `APALACHE_JAR`, or `APALACHE_BIN` is present—Snowcat-typed Apalache/Z3 symbolic inductive checks).

---

## Repository Organization

* **`crates/`** — 42 focused Rust crates. Each crate contains a local `AGENTS.md` describing the exact state or lock domain it owns, the invariants its callers rely on, and the tests covering its boundaries.
* **`config/`** — Canonical static deployment configurations (`config/reference.toml` and `config/production.template.toml`) validated against host topology, CPU isolation, and multi-UID ORS bounds.
* **`specs/`** — Normative architecture and module specifications:
  * [`specs/INDEX.md`](specs/INDEX.md) — Architecture overview and subsystem reading roadmap.
  * [`specs/implementation/`](specs/implementation/README.md) — All 53 per-module behavioral contracts, state machines, and resource bounds.
  * [`specs/contracts/`](specs/contracts/README.md) — Machine-readable JSON schemas and language-independent golden test vectors.
  * `specs/evidence/` — Historical acceptance archives with byte-exact SHA-256 manifests (**never edit archived files**).
* **`tla/`** — 21 model families (`40` finite-state TLC 2.19 models and `11` Snowcat-typed symbolic Apalache 0.63.0 `IndInv` models) anchored via `anchors.lock` SHA-256 digests to concrete Rust source lines (see [`tla/ROADMAP.md`](tla/ROADMAP.md)).
* **`scripts/`** — Rust, Linux IPC/affinity, contract, documentation, and TLA+ verification runners (see [`scripts/README.md`](scripts/README.md)).
* **`assets/`** — Hand-authored architectural SVG diagrams ([`assets/architecture.svg`](assets/architecture.svg) and [`assets/order-lifecycle.svg`](assets/order-lifecycle.svg)) verified down to 320px viewport width.

---

## Making a Change

### 1. Start at the Specification Boundary (Code-Doc Lockstep)

Before changing observable behavior or public APIs, identify the single writer or fine-grained spinlock domain that owns the transition:

1. Update the governing specification in [`specs/implementation/`](specs/implementation/README.md), the crate's local `AGENTS.md`, and its crate-root `//!` rustdoc in lockstep with concrete inputs, state transitions, refusal/doubt/freeze semantics, and acceptance criteria.
2. Add positive and negative unit or integration tests in the owning crate covering the target transition and boundary conditions.
3. Implement the change in safe, checked Rust (`[workspace.lints.rust] unsafe_code = "forbid"` outside the audited OS/clock/control, zero-copy/SIMD wire, and seal-verified `memfd` attachment boundaries under `#![deny(unsafe_op_in_unsafe_fn)]`, checked integer ticks/lots/`Fixed` math with zero floats, and zero logging, allocation, or parsing while holding any spinlock).

### 2. Keep Cross-Language Contracts Synchronized

When touching SHM IPC binary layouts, static configuration schemas, instrument catalog economics, or risk formulas:

* Update the matching JSON schema or golden vector in [`specs/contracts/`](specs/contracts/README.md) alongside its `scripts/verify-*.py` validator.
* Run the independent Python contract suite:
  ```sh
  python3 -m venv /tmp/tickwire-contracts
  /tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
  /tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
  ```

### 3. Check Formal Models on Ordering, Lock, or Recovery Changes

When modifying `REQ`/`RESP` spinlock ordering, order admission, `StpBook` linking/retirement, shared-memory ring reclamation, JOIN/DOP handshakes, clock calibration, or stateless Exchange REST (`Account + OpenOrders` + `tw1...`) bootstrap:

* Check [`tla/ROADMAP.md`](tla/ROADMAP.md) for the model family covering your crate.
* Verify whether your change alters any modeled action or invariant, update the model and source anchors if needed, and run:
  ```sh
  ./scripts/check-tla.sh <model-family>
  ```

### 4. Report Performance Honestly

* Separate cold startup costs (static config validation, ELF sealing, cgroup/CPU pinning, catalog digests, `Dialer` DNS/TLS handshakes, cold REST snapshot bootstrap) from steady-state hot-path costs (per-quote decode, inline ring borrow, `REQ` risk/STP/router dispatch, `RESP` single-pass `JsonScan`).
* Never present structural code estimates as empirical latency numbers. When adding benchmark results, always record the exact command, CPU model, OS/kernel environment, CPU pinning/load conditions, and the exact start/stop measurement boundary.

---

## Pre-Submission Checklist

Every change must pass formatting, workspace compilation and lint checks, unit/integration tests, contract checks, and documentation link verification:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
python3 scripts/verify-documentation.py
```

See [`AGENTS.md`](AGENTS.md) for the concise summary of non-negotiable engineering invariants.
