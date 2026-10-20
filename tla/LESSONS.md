# lessons.md — case law: how a TLC violation fools you, and the rules that prevent it

> **Read this before classifying any violation as a finding.** Every rule below comes
> from a real verification mistake (a false positive that looked airtight, or a true
> positive nearly thrown away). They are written repo-agnostic on purpose — they
> transfer to any codebase. When you start a new repo, `init` copies this file into the
> workspace as `LESSONS.md`; grow it there with that repo's own refutations.

The single most common failure mode: **TLC faithfully reports a violation of a model
that is looser than the system** — the model omitted a guard the code actually has. The
violation is then information about the *model*, not the *system*. Most of the rules
below are ways to catch that before you call it a bug. The exceptions (R4) are the
mirror case: a *faithful* model whose finding is real-but-conditional on a fact only the
owner holds.

---

## Table of Contents

- The triage checklist (before reporting any violation as candidate+)
- R1 — Denotation, witness, obligations, conditionality
- R2 — A claimed race needs its threads
- R3 — Non-terminal states, and mismatched gates over different sets
- R4 — A faithful model can still raise a conditional finding
- R5 — A frozen assumption is a precondition too
- The counterpoint — what a finding that survives the checklist looks like

## The triage checklist (run before reporting any violation as candidate+)

This checklist is also the procedure the **`review`** command runs *adversarially* — a fresh
pass (ideally an independent agent) that tries to *refute* a candidate: survives → confirmed
(true positive); falls to any item → retired/refuted (false positive, model artifact, or
hallucination).

1. **Denotation.** What does each modeled value *denote*, per its producer? (R1.1)
2. **Witness.** Does the violation carry a *semantic* witness — data actually
   wrong/missing/lost — not just a label difference? (R1.2)
3. **Omitted obligations.** Whose obligations (caller contracts, dispatch invariants,
   status fallbacks, producer ordering) did the model assume away? Did you search *all*
   relevant trees, including out-of-component callers? (R1.3, R5)
4. **Conditionality.** Is severity stated as *conditional on* a precisely-named open
   precondition, rather than escalated on an assumed answer? (R1.4)
5. **Designed regime.** Is the bound you'd refute on actually closed, or is the design
   moving it? (R4.4–R4.5)

If every box is clean and the violation still stands, you have a credible candidate.

---

## R1 — Denotation, witness, obligations, conditionality

A "staleness" / "lost data" / "violated invariant" finding is the easiest to get wrong.

- **R1.1 — Denotation before invariant.** Before asserting any watermark/counter
  invariant (`X ≥ Y`), establish *from the producer* what `X` denotes: a sync **progress**
  marker (a low value means data is lagging / missing) or a content **boundary** marker
  (a low value means there simply is no data past it). Only the first kind can be
  "stale". A value below a threshold is not automatically "behind". Write the denotation,
  with the producer's `file:line`, into the `var ↔ code` map.

- **R1.2 — Staleness needs a witness.** A "stale read" / "lost write" claim must exhibit a
  concrete *write in the gap* — data that exists in `(served, needed]` and is not
  returned. A violation whose trace shows only a *version-label* difference, with no data
  actually different, has proven nothing. Encode the witness in the model (e.g. a
  `has_data_in_gap` flag the violating state must set) so a label-only "violation" cannot
  fire.

- **R1.3 — Model the caller's obligations (they may live outside the component).** The
  guard that makes the system safe may be a *producer/caller contract* enforced in
  **another module of this repo** (or, if `PROFILE.md` lists it, another source root) — an
  ordering guarantee, a push-before-read, a value semantics. Enumerate the API's obligations
  (ordering, value semantics, cadence) and either model them or list them as explicit
  `ASSUME`-level open questions **before** classifying. Search the rest of the repo (and the
  roots the profile lists) for the producer first — a missing guard found one directory over
  flips a "bug" to an artifact. This extends to **tests and design docs**: the tests show
  what the API *permits*; the design docs show where the bound is *headed*. (If the producer
  genuinely lives in a *different repo* than the one the skill is scoped to, say so as an
  open question and have the user add that root to `PROFILE.md` — don't silently reach
  outside the repo.)

- **R1.4 — Hold severity until the precondition is discharged.** When a finding turns on
  one fact you cannot settle from the code, classify it **candidate, conditional on that
  fact**, pose the fact as one precise owner question, and *do not* escalate on the
  assumed answer. The answer **is** the finding — it cuts to confirmed or refuted either
  way. Conditionality is honesty about where the truth lives, not pessimism.

---

## R2 — A claimed race needs its threads

**The mistake.** Model a non-atomic read-modify-write as racing on two threads and
"confirm" a lost update — when in production every caller for that object is serialized
on a single owning run-to-completion worker, so the race cannot occur.

**The rule.** A threading precondition discharged "from the caller code" must survive the
**entire** dispatch + transport stack: dispatch keys, worker pinning, reactor/ack
delivery. The model's concurrency must be the *runtime's* concurrency, not the source
file's apparent one. (Mirror image in R5.)

---

## R3 — Don't collapse non-terminal states; don't conflate two gates over different sets

**The mistakes.** (a) Map a *non-terminal* status that triggers a fallback (e.g. "no full
page → fall back to a quorum read") to a *terminal* failure — manufacturing a
user-visible error the system doesn't have. (b) Read one threshold (`count ≥ k`) as a
success gate when it actually filters a *different set* than you assumed — manufacturing
a contradiction.

**The rule.** For every modeled status/gate, verify (a) terminal vs non-terminal — what
retry/fallback consumes it — and (b) *which set* a threshold counts. Two thresholds over
different sets are not "the same check done twice".

---

## R4 — A *faithful* model can still raise a conditional finding (and "the answer is the finding" cuts toward refuted too)

Sometimes the spec is exact — not looser than the code — but its *domain* is bounded by an
external policy the model can't contain (an allocation rule, a service/provisioning
constraint, a design intent). TLC's violation over the *unconstrained* domain is then a
candidate **conditional on that bound**.

- **R4.1 — Discharge a domain bound with its owner, not by re-reading the code.** When the
  code is already faithful, re-reading it won't resolve the finding; the bound lives with
  whoever owns the allocation/deployment/design rule. The bound may be a **design**
  invariant (an encoding only needs N bits) *or* a **deployment** invariant (a VM size ×
  config guarantees a cap) — both are owner-held facts, settled in one precise question
  with a single concrete witness.

- **R4.2 — Symmetric honesty.** The same conditional-severity discipline that graduates a
  finding to **confirmed** when the owner says "yes, reachable" discharges it to
  **refuted (safe by design)** when the owner says "unreachable by design". The owner's
  answer flips the status, not your prior.

- **R4.3 — Keep the demonstration as a tripwire.** A refuted-by-bound config is not
  deleted. Retained as a *bound demonstration*, it shows exactly what breaks if the design
  ever admits the excluded input — a regression tripwire against a future change that
  widens the domain. (Pair it with a hardening note: assert the bound in code.)

- **R4.4 — "Closed" means the design isn't moving it either.** A finding is "discharged
  from code" only when the bounding value is in-tree **and not slated to change by the
  design/roadmap**. A constant the current binary happens to hold (a level count, a size
  cap) is **not** closed if a design doc plans to widen it. Before downgrading on a
  code-level bound, **read the design doc / roadmap for that bound**, not just the source
  and tests. When unsure whether a bound is closed, it is an owner question — keep the
  finding candidate (R1.4).

- **R4.5 — Model the designed regime, not only the deployed one.** When a subsystem is
  mid-migration, also check the property under the *target* design. A defect the current
  partial deployment masks but the intended design exposes is a high-value
  **forward-looking candidate** — caught before it ships, usually with a cheap fix.

---

## R5 — A *frozen assumption* is a precondition too: verify it against the topology, not a comment

**The setup.** A level froze a concurrency assumption — "all writers live on one worker;
the write path is serialized under one lock" — quoting a code comment to make progress.
The assumption turned out false: one shared instance, sharded by a key *orthogonal* to
the thread-dispatch key, is reached from multiple workers concurrently; the per-shard
lock exists precisely because of that. A lock-free pre-check outside the lock was a real
TOCTOU.

**Why this is the mirror image of R2.** R2: a *claimed race* must survive the dispatch
stack — there the runtime concurrency was *less* than the source suggested. R5: a
*claimed serialization* was not real — the runtime concurrency was *more* than a comment
claimed. The discipline is identical and symmetric: **the model's concurrency must be the
runtime's concurrency** — applied to assumptions, not just findings.

- **R5.1 — The checklist cuts both ways.** A violation that has *already* enumerated its
  rescue/recovery paths, carried its semantic witness, and pinned its one open fact
  converts to **confirmed** the moment that fact lands — with no re-litigation. Rigor
  spent refuting false positives is rigor banked for confirming the real one. (A finding
  that survives all of R1–R5 is credible; an *assumption* that hasn't faced the same
  scrutiny is not yet earned.)

- **R5.2 — A frozen assumption is a hypothesis, not a fact.** Freezing "it's serialized /
  single-writer" to make progress is fine, but it carries a debt: the level whose concern
  *is* that assumption must **re-derive it from the ownership + sharding topology** — who
  owns the instance? how is it sharded? is the shard key the same as the thread-dispatch
  key? — not from a code comment. A comment is evidence of *intent*, not of runtime
  behavior, and it goes stale when the design changes.

- **R5.3 — The lock that "shouldn't be needed" is the tell.** If the code holds a
  per-shard lock and takes it on *read* paths while a comment claims single-writer,
  believe the lock: it is there because the authors expect concurrent access. Treat the
  contradiction as a finding to discharge, not noise to ignore.

---

## The counterpoint — what a finding that *survives* the checklist looks like

The rules above mostly retire false positives. A finding that graduates candidate →
confirmed did four things the refuted ones didn't:

1. **Denotation was concrete (R1.1).** Every modeled value mapped to a plain
   counter/flag/state — no watermark whose semantics could be misread.
2. **The violation carried a behavior witness (R1.2).** A difference in what the system
   *does* (e.g. a computed-but-never-delivered response; a duplicated entry), not a label.
3. **Caller obligations were enumerated *before* classification (R1.3).** Every wake-up /
   recovery / rescue path was grep-exhausted; the model was made *more generous* than the
   code, so the violation survives the "your model starved the system" rebuttal.
4. **Severity stayed conditional until discharge (R1.4).** One precisely-stated owner fact
   open; nothing escalated on an assumed answer; the owner's affirmative — not your
   optimism — flipped the status.

Make your model **more generous** than the code where you're unsure (extra recovery
ticks, an adversary bounded only by the protocol). A violation that survives a generous
model is the dual of the looser-than-code failure shape — and that's what makes it
credible.
