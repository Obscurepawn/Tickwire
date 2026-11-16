# Formal Verification with TLA+ and Apalache

Tickwire pairs exhaustive finite-state TLA+ model checking (`TLC 2.19`) with unbounded 64-bit / 128-bit symbolic SMT induction (`Apalache 0.63.0 + Z3`) and per-line SHA-256 source-anchor lockfiles (`anchors.lock`) to verify concurrency, spinlock hierarchy, shared-memory reclamation, rate-limit admission, doubt resolution, and stateless Exchange-REST recovery across the 3-process Linux topology.

See the [November 2026 Stateless Multi-UID ORS Audit](audits/2026-11-16/README.md) for verification scope, dual-engine proof architecture, and subsystem coverage, and [`MEMORY.md`](MEMORY.md) for the per-level verification ledger.

## Running the Verification Suite

From the workspace root, verify one or more model families in parallel (requires a Java runtime):

```sh
./scripts/check-tla.sh strategy_intake request_budget
./scripts/check-tla.sh account query_desk hotpath_decoupling
```

Or run the complete formal verification suite across all 21 model families:

```sh
./scripts/check-tla.sh
```

For each selected family, `./scripts/check-tla.sh` enforces three verification gates:

1. **Zero-Drift Source Anchors (`anchors.lock`):** Verifies that every cited core-crate Rust source line recorded in `tla/execution_models/<family>/*/02_models/anchors.lock` matches its current SHA-256 digest (`0` changed or missing lines).
2. **Exhaustive Finite-State TLC (`tla/tools/tla2tools.jar`):** Verifies the pinned TLC SHA-256 digest, stages every `.tla` module and `.cfg` file into isolated scratch directories, schedules `(model, .cfg)` runs Longest-Processing-Time-First across CPU cores (`TLA_JOBS`, `TLA_MAX_HEAP`), and verifies `0 states left on queue`, `0` invariant violations, and `100%` nonzero action coverage via [`scripts/tlc_coverage.py`](../scripts/tlc_coverage.py).
3. **Unbounded Symbolic Induction (`Apalache + Z3`):** When Apalache (`tla/tools/apalache/bin/apalache-mc` or `APALACHE_BIN`) is installed, runs Snowcat static typechecking and inductive verification (`Init => IndInv` and `IndInv /\ Next => IndInv'`) across every Snowcat-typed symbolic specification (`L*_Sym*.tla`) in the selected families.

Run the coverage-aggregator and runner unit tests with:

```sh
python3 -m unittest scripts/test_tlc_coverage.py
```

## Maintaining Source Alignment

Consult the [Model Family Roadmap](ROADMAP.md) to locate the TLA+ and Apalache models governing any subsystem boundary, and [`LESSONS.md`](LESSONS.md) before triaging counterexamples. Whenever Rust code changes event ordering, spinlock acquisition, admission gating, shared-memory reclamation, or stateless Exchange-REST bootstrap logic, update the corresponding specification and re-baseline `anchors.lock` before committing.
