# Run Log (`L1_SymOrsSessionRouting`)

## 1. Finite Explicit-State TLC (`BoundedMode = TRUE`)

```bash
./scripts/check-tla.sh strategy_intake
```

- `L1_SymOrsSessionRouting.cfg`: `172,114` generated states, `15,936` distinct states, diameter `17`, `0 states left on queue`, `14 / 14` `Next` actions (`+ Init`) nonzero (`3s` wall time, PASS).

## 2. Symbolic Apalache + Z3 (`BoundedMode = FALSE` via `ConstInit`)

```bash
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh typecheck \
  tla/execution_models/strategy_intake/L1_sym_ors_session_routing/02_models/L1_SymOrsSessionRouting.tla
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh inductive \
  tla/execution_models/strategy_intake/L1_sym_ors_session_routing/02_models/L1_SymOrsSessionRouting.tla \
  --cinit=ConstInit --indinv=IndInv
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh bmc \
  tla/execution_models/strategy_intake/L1_sym_ors_session_routing/02_models/L1_SymOrsSessionRouting.tla \
  --cinit=ConstInit --inv=SafetyInv --length=8
```

| Engine & Mode | Obligation | Verdict | Wall Time |
| --- | --- | --- | ---: |
| Apalache `0.63.0` Snowcat | `typecheck` | `Type checker [OK]` | 1.86s |
| Apalache + Z3 (`QF_LIA`) | `inductive:base` (`Init => IndInv`) | `NoError` (`EXITCODE: OK`) | 2.96s |
| Apalache + Z3 (`QF_LIA`) | `inductive:step` (`IndInv /\ Next => IndInv'`) | `NoError` (`EXITCODE: OK`) | 3.39s |
| Apalache + Z3 (`QF_LIA`) | `bmc --length=8` (`SafetyInv`) | `NoError` (`EXITCODE: OK`) | 28.14s |
