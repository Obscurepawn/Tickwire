# Formal Verification with TLA+ and Apalache

Tickwire pairs exhaustive finite-state TLA+ model checking (`TLC 2.19`, `tla2tools.jar` `v1.7.4` release) across **21 model families and 40 TLA+ models** with unbounded 64-bit / 128-bit symbolic SMT induction (`Apalache 0.63.0 + Z3`) across **11 Snowcat-typed symbolic `IndInv` models** and per-line SHA-256 source-anchor lockfiles (`44,618` tracked Rust lines across `02_models/anchors.lock`) to verify concurrency, spinlock hierarchy, shared-memory reclamation, rate-limit admission, doubt resolution, and stateless Exchange-REST recovery across the 3-process Linux topology.

See the [November 2026 Stateless Multi-UID ORS Audit](audits/2026-11-16/README.md) for verification scope, dual-engine proof architecture, and subsystem coverage, and [`MEMORY.md`](MEMORY.md) for the per-level verification ledger.

## Running the Verification Suite

From the workspace root, verify one or more model families in parallel (requires a Java runtime):

```sh
./scripts/check-tla.sh strategy_intake request_budget
./scripts/check-tla.sh account query_desk hotpath_decoupling
```

Or run the complete formal verification suite across all 21 model families (`40` models, `11` Apalache Snowcat symbolic `IndInv` models):

```sh
./scripts/check-tla.sh
```

For each selected family, `./scripts/check-tla.sh` enforces three verification gates:

1. **Zero-Drift Source Anchors (`anchors.lock`):** Verifies that every cited core-crate Rust source line recorded in `tla/execution_models/<family>/*/02_models/anchors.lock` matches its current SHA-256 digest (`0` changed or missing lines; opt-out via `TLA_SKIP_ANCHORS=1`).
2. **Exhaustive Finite-State TLC (`tla/tools/tla2tools.jar`):** Verifies the pinned `tla2tools.jar` (`v1.7.4` / `TLC 2.19`) SHA-256 digest, stages every `.tla` module and `.cfg` file into isolated scratch directories, schedules `(model, .cfg)` runs Longest-Processing-Time-First across CPU cores (`TLA_JOBS`, `TLA_MAX_HEAP`), and verifies `0 states left on queue`, `0` invariant violations, and `100%` nonzero action coverage via [`scripts/tlc_coverage.py`](../scripts/tlc_coverage.py).
3. **Unbounded Symbolic Induction (`Apalache 0.63.0 + Z3`):** When Apalache (`tla/tools/apalache/lib/apalache.jar`, `APALACHE_JAR`, or `APALACHE_BIN`; opt-out via `TLA_SKIP_APALACHE=1`) is installed, runs Snowcat static typechecking (`@type:`) plus `base` (`Init => IndInv`), `step` (`IndInv /\ Next => IndInv'`), and safety `entailment` checks across all `11` Snowcat-typed symbolic `IndInv` specifications (`L*_Sym*.tla`) in the selected families.

Run the coverage-aggregator and runner unit tests with:

```sh
python3 -m unittest scripts/test_tlc_coverage.py
```

## Maintaining Source Alignment

Consult the [Model Family Roadmap](ROADMAP.md) to locate the TLA+ and Apalache models governing any subsystem boundary, and [`LESSONS.md`](LESSONS.md) before triaging counterexamples. Whenever Rust code changes event ordering, spinlock acquisition, admission gating, shared-memory reclamation, or stateless Exchange-REST bootstrap logic, update the corresponding specification and re-baseline `anchors.lock` before committing.
