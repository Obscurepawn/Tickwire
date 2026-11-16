# Run Log — `L1_SymSharedLimiter`

## 1. Anchor Verification (`anchors.sh`)

```text
ok execution_models/request_budget/L0_shared_ors_limiter: 1099 line anchor(s) -> execution_models/request_budget/L0_shared_ors_limiter/02_models/anchors.lock
ok execution_models/request_budget/L1_sym_shared_limiter: 1037 line anchor(s) -> execution_models/request_budget/L1_sym_shared_limiter/02_models/anchors.lock
anchors.sh check: 2136 anchor line(s) across 2 lock(s); 0 changed
```

## 2. Apalache / Z3 Unbounded 64-Bit & 128-Bit Symbolic Proofs (`v0.63.0`)

- **Snowcat Typecheck (`run_apalache.sh typecheck`):**
  - `TypeCheckerSnowcat`: `All expressions are typed` — `EXITCODE: OK` (`2.40s`).
- **Unbounded Inductive Proof (`run_apalache.sh inductive --cinit=ConstInit --indinv=IndInv --inv=SafetyConjunction`):**
  1. `inductive:base (Init => IndInv)`: `112` verification conditions, `NoError` (`5s`).
  2. `inductive:step (IndInv /\ Next => IndInv')`: `24` extracted symbolic transitions, `NoError` (`11s`).
  3. `inductive:entailment (IndInv => SafetyConjunction)`: `NoError` (`5s`).
- **Bounded Symbolic Model Check (`run_apalache.sh bmc --cinit=ConstInit --inv=SafetyConjunction --length=2`):**
  - `NoError` (`75s`).

## 3. Dual-Engine Finite-State TLC Action-Coverage Run (`BoundedMode = TRUE`)

- `L1_SymSharedLimiter.cfg` (`AllowFreeze = FALSE`): `4,138` states generated, `1,394` distinct states found, `0` states left on queue (`1s`).
- `L1_SymSharedLimiter_ban.cfg` (`AllowFreeze = TRUE`): `4,011` states generated, `1,337` distinct states found, `0` states left on queue (`1s`).
- Aggregated TLC coverage (`tlc_coverage.py`): `runs = 2`, `entries = 14` (`100%` nonzero action coverage across all 14 `Init` + `Next` actions), `model_sha256 = f2794425fc513fa3762d3e9dc9525322e1b995d642a1a46e49e2f0ba1f85ab02`.
