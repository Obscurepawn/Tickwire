# Run Log (`L1_SymDualThreadSwitchover`)

## 1. Finite Explicit-State TLC (`BoundedMode = TRUE`, `CHECK_DEADLOCK TRUE`)

```bash
./scripts/check-tla.sh transport
```

- `L1_SymDualThreadSwitchover.cfg`: `6,802` generated states, `2,056` distinct states, diameter `30`, `0 states left on queue`, `0` deadlocks, `25 / 25` `Next` actions (`+ Init`) nonzero (`1s` wall time, PASS).

## 2. Symbolic Apalache + Z3 (`BoundedMode = FALSE` via `ConstInit`)

```bash
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh typecheck \
  tla/execution_models/transport/L1_sym_dual_thread_switchover/02_models/L1_SymDualThreadSwitchover.tla
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh inductive \
  tla/execution_models/transport/L1_sym_dual_thread_switchover/02_models/L1_SymDualThreadSwitchover.tla \
  --cinit=ConstInit --indinv=IndInv
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh bmc \
  tla/execution_models/transport/L1_sym_dual_thread_switchover/02_models/L1_SymDualThreadSwitchover.tla \
  --cinit=ConstInit --inv=SafetyInv --length=8
```

| Engine & Mode | Obligation | Verdict | Wall Time |
| --- | --- | --- | ---: |
| Apalache `0.63.0` Snowcat | `typecheck` | `Type checker [OK]` | 1.41s |
| Apalache + Z3 (`QF_LIA`) | `inductive:base` (`Init => IndInv`) | `NoError` (`EXITCODE: OK`) | 3.21s |
| Apalache + Z3 (`QF_LIA`) | `inductive:step` (`IndInv /\ Next => IndInv'`) | `NoError` (`EXITCODE: OK`) | 3.74s |
| Apalache + Z3 (`QF_LIA`) | `bmc --length=8` (`SafetyInv`) | `NoError` (`EXITCODE: OK`) | 5.35s |
