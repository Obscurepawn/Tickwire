# Verification Case Law (`LESSONS.md`)

> **Read this before classifying any TLC or Apalache counterexample as a finding.** Every rule below comes from a concrete formal-verification false positive or subtle true positive across Tickwire's `21` model families (`40` TLC models and `11` Apalache Snowcat symbolic `IndInv` models). Apply the triage checklist first so model artifacts are separated cleanly from real production defects.

The single most common failure mode is **a model looser than the production code** — omitting a caller invariant, lock recheck, or dispatch serialization that the implementation enforces. Such a counterexample diagnoses the *model*, not the *system*.

---

## Table of Contents

- [The Triage Checklist](#the-triage-checklist)
- [R1 — Denotation, Witness, Obligations, Conditionality](#r1--denotation-witness-obligations-conditionality)
- [R2 — A Claimed Race Needs Its Runtime Threads](#r2--a-claimed-race-needs-its-runtime-threads)
- [R3 — Non-Terminal States and Mismatched Threshold Sets](#r3--non-terminal-states-and-mismatched-threshold-sets)
- [R4 — Domain Bounds and Forward-Looking Regimes](#r4--domain-bounds-and-forward-looking-regimes)
- [R5 — Verify Frozen Concurrency Assumptions Against Topology](#r5--verify-frozen-concurrency-assumptions-against-topology)
- [R6 — Tickwire Stateless ORS & Symbolic `QF_LIA` Lessons](#r6--tickwire-stateless-ors--symbolic-qf_lia-lessons)

---

## The Triage Checklist

Run this adversarial checklist before promoting any counterexample to `candidate` or `confirmed`:

1. **Denotation (R1.1):** What does each modeled variable denote at its producer (`file:line`) — a progress watermark or a content boundary?
2. **Semantic Witness (R1.2):** Does the counterexample exhibit lost, corrupted, double-counted, or over-committed state rather than a purely internal label difference?
3. **Caller Obligations (R1.3, R5):** Which producer, router, or spinlock invariants in sibling crates constrain the input domain?
4. **Conditionality (R1.4):** If a finding depends on an external deployment or venue bound, state severity conditionally on that exact precondition.
5. **Designed Regime (R4.4–R4.5):** Check both current constants and the governing specification under [`specs/implementation/`](../specs/implementation/README.md).

---

## R1 — Denotation, Witness, Obligations, Conditionality

- **R1.1 — Denotation before invariant:** Establish from the producer's `file:line` whether a counter or sequence number denotes progress or a closed upper bound, and record that mapping in the `var <-> code` header.
- **R1.2 — Staleness and loss require a concrete witness:** Encode an explicit witness predicate (for example, an unapplied fill delta, over-released hold, or aliased borrow slot) so label-only divergences cannot masquerade as safety violations.
- **R1.3 — Model cross-crate caller obligations:** Check caller contracts across the workspace (`tickwire-runtime`, `tickwire-account`, `tickwire-transport`, `tickwire-quota`, `tickwire-shm`) before classifying a module-local trace as reachable.
- **R1.4 — Hold severity conditional until discharged:** State open domain assumptions explicitly rather than escalating on an unverified guess.

---

## R2 — A Claimed Race Needs Its Runtime Threads

A concurrency race claimed on a struct method must be reachable through the actual thread topology (`REQ` spin thread, `RESP` epoll thread, and cold `housekeeping_cpus` workers such as `Dialer` and `OrsQueryDesk` / `QueryDesk`). Single-writer thread-local state cannot race with itself; shared cross-thread structures (`OrderManager` slots, `LockedConnection`, `DialBaton`, `SharedRateLimiter`) must be modeled at their exact atomic/spinlock boundaries.

---

## R3 — Non-Terminal States and Mismatched Threshold Sets

Verify for every status code and counter threshold:
1. Whether a rejection or uncertain send status is terminal or routes into `OrsQueryDesk` doubt resolution (`StatusAsk` / `Cancel` retry).
2. Exactly which set a threshold counts (for example, `order_qty - projected_fill_qty` vs. settled fills, or per-account `ORDER` fixed windows vs. cross-account shared IP rolling sub-buckets).

---

## R4 — Domain Bounds and Forward-Looking Regimes

- **R4.1–R4.3 — Retain boundary tripwires:** When safety holds under an explicit protocol or configuration invariant (such as `RingCapacity >= 2` or `max_order_age_ns = 23_000`), encode the assumption in `ASSUME` / `ValidConfig` and retain boundary `.cfg` files as regression tripwires.
- **R4.4–R4.5 — Verify target production topology:** Model the stateless multi-UID ORS execution pipeline directly rather than relying on retired single-account assumptions.

---

## R5 — Verify Frozen Concurrency Assumptions Against Topology

- **R5.1–R5.3 — Believe locks and atomic orderings over stale prose:** Whenever a structure carries per-slot spinlocks (`LockedOrder`, `LockedConnection`, `OrsStpBook` / `StpBook`) or `AcqRel` generation tags, derive concurrency invariants from the actual lock hierarchy (`LockedOrder` and `LockedConnection` never overlap; `OrsStpBook` is acquired strictly as a leaf under `LockedOrder`, while send-path self-trade checks read atomic BBO prices lock-free).

---

## R6 — Tickwire Stateless ORS & Symbolic `QF_LIA` Lessons

1. **Post-lock slot identity recheck on power-of-two `OrderManager` rings:** Because `slot = seqnum & mask` aliases modulo `capacity` once terminal orders transition immediately to `RETIRED` without waiting on late fee amendments, any lock acquisition by slot index must re-verify `(strat_id, seqnum)` inside `LockedOrder` before mutating order state ([`account`](execution_models/account/ROADMAP.md)).
2. **Separate order-status projections (`ProjectedFill`), trade executions (`Fill`), and fee amendments (`amendment = true`):** Deduplicating trades in `FillRecordPool` while allowing `amendment = true` fee/rebate patches prevents both double position application and false terminal conflicts after immediate `OrderManager` slot retirement ([`account`](execution_models/account/ROADMAP.md), [`market_making_microstructure`](execution_models/market_making_microstructure/ROADMAP.md)).
3. **Linearize `u32` epoch modulo over 64-bit nanosecond domains in Apalache/Z3 (`QF_LIA`):** In `AtomicFixedWindow::try_take` (`epoch = ((now_ns / 1_000_000_000) as u32)`), nested Euclidean division plus modulo `((t \div 10^9) % 2^32)` over existential `u64` timestamps stalls SMT modulo elimination. Because `0 <= (t \div 10^9) < 5 * 2^32` across all `t \in 0 .. 2^64 - 1`, expressing `WrapU32` as a 5-branch piecewise-linear subtraction is bit-for-bit identical over the entire `u64` domain and solves in seconds ([`request_budget`](execution_models/request_budget/L1_sym_shared_limiter/04_counterexamples.md)).
4. **Observation-point precision across `REQ`, `RESP`, and `Dialer`:** Model `max_order_age_ns` (`23,000 ns`) staleness checks at `REQ` intake before route/quota spend, and verify connection switchover across `DialBaton` + maintenance `notice` (`64008`) draining so in-flight replies on retiring sockets are never dropped ([`strategy_intake`](execution_models/strategy_intake/ROADMAP.md), [`hotpath_decoupling`](execution_models/hotpath_decoupling/ROADMAP.md)).
