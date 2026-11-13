# Contributing to Tickwire

Tickwire is engineered as a high-assurance trading runtime where state ownership, crash recovery, and resource bounds are treated as first-class invariants rather than best-effort conventions. This guide explains how the repository is organized, how to set up a development environment, and how to validate changes across Rust code, machine-readable contracts, and formal TLA+ models.

## Development Environment

Tickwire targets **Linux** in production and process-level integration tests. Key subsystems rely on Linux-specific kernel primitives:

* **`memfd_create` and file sealing** (`F_ADD_SEALS`) for immutable market snapshots, single-writer shared-memory rings, and sealed worker ELF images.
* **Unix domain sockets** with `SCM_RIGHTS` descriptor passing and `SO_PEERCRED` peer credential verification.
* **`CLOCK_BOOTTIME`** for suspend-aware monotonic time domains.
* **cgroup v2** and `sched_setaffinity` for optional worker memory, swap, CPU, and NUMA isolation.

### Required Tools

1. **Rust toolchain:** Installed automatically via `rust-toolchain.toml` (Rust 2024 edition). Always pass `--locked` to Cargo commands so builds and tests use the exact dependency graph in `Cargo.lock`.
2. **System utilities:** OpenSSL headers/libraries and GNU `binutils` (`strip`, used by Linux process harnesses when preparing worker binaries).
3. **Python 3.11+:** Used for documentation link checking, golden-vector verification (`jsonschema` in `scripts/requirements-design.txt`), and multi-process demonstration launchers.
4. **Optional verification tools:**
   * **Docker:** Required for `./scripts/check-linux.sh` and containerized lab runs.
   * **Java (JRE 11+):** Required for `./scripts/check-tla.sh` (executes the pinned TLC 1.7.4 model checker in `tla/tools/tla2tools.jar`).

---

## Repository Organization

* **`crates/`** — 55 single-purpose Rust crates. Each crate contains a local `AGENTS.md` describing the exact state it owns, the invariants its callers rely on, and the tests that cover its boundaries.
* **`specs/`** — Normative architecture and module specifications:
  * [`specs/INDEX.md`](specs/INDEX.md) — Workflow-oriented entry point.
  * [`specs/implementation/`](specs/implementation/README.md) — Per-module behavioral contracts, state machines, and resource bounds.
  * [`specs/contracts/`](specs/contracts/README.md) — Machine-readable JSON schemas and language-independent golden test vectors.
  * `specs/evidence/` — Historical acceptance archives with byte-exact SHA-256 manifests (**do not edit archived files**).
* **`tla/`** — Finite-state TLA+ specifications and TLC coverage audits anchored to concrete Rust source revisions (see [`tla/ROADMAP.md`](tla/ROADMAP.md)).
* **`scripts/`** — Rust, Linux IPC, contract, documentation, and TLA+ verification runners (see [`scripts/README.md`](scripts/README.md)).
* **`assets/`** — Hand-authored architectural SVG diagrams verified down to 320px viewport width.

---

## Making a Change

### 1. Start at the Specification Boundary

Before changing observable behavior, identify the single component that owns the transition:

1. Update the relevant specification in [`specs/implementation/`](specs/implementation/README.md) with concrete inputs, state transitions, error/degradation behavior (`Refusal` vs. `Freeze` vs. `Poison`), and acceptance criteria.
2. Add a minimal failing unit or integration test in the owning crate that reproduces the target scenario.
3. Implement the change in safe, checked Rust (`#![forbid(unsafe_code)]`, checked integer/fixed-point math) and confirm all existing invariants still hold.

### 2. Keep Cross-Language Contracts Synchronized

When touching binary layouts, CBOR encodings, configuration schemas, catalog economics, or risk formulas:

* Update the matching JSON schema or golden vector in [`specs/contracts/`](specs/contracts/README.md).
* Run the independent Python validators:
  ```sh
  python3 -m venv /tmp/tickwire-contracts
  /tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
  /tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
  ```

### 3. Check Formal Models on Ordering or Recovery Changes

When modifying order submission, WAL framing, recovery closures, shared-memory ring reclamation, JOIN handshakes, clock calibration, or venue reconciliation:

* Check [`tla/ROADMAP.md`](tla/ROADMAP.md) for the model family covering your crate.
* Verify whether your change alters any modeled action or invariant, update the model and source anchors if needed, and run:
  ```sh
  ./scripts/check-tla.sh <model-family>
  ```

### 4. Report Performance Honestly

* Separate cold-path costs (startup configuration, TLS handshakes, snapshot allocation, `fsync` directory setup) from steady-state hot-path costs (per-quote decode, ring borrow, candidate risk evaluation).
* Never present structural code estimates as empirical latency numbers. When adding benchmark results, always state the exact command, CPU model, OS/kernel environment, pinning/load conditions, and the exact start/stop measurement boundary.

---

## Pre-Submission Checklist

Every change should pass formatting, workspace compilation and lint checks, unit/integration tests, and documentation link verification:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
python3 scripts/verify-documentation.py
```

See [`AGENTS.md`](AGENTS.md) for the concise summary of non-negotiable engineering invariants.
