# L1 Symbolic 64-Bit / 128-Bit ORS Order Lifecycle Run Log

## Verification Commands

```sh
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh inductive \
  tla/execution_models/account/L1_sym_ors_order_lifecycle/02_models/L1_SymOrsOrderLifecycle.tla \
  --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit

/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh bmc \
  tla/execution_models/account/L1_sym_ors_order_lifecycle/02_models/L1_SymOrsOrderLifecycle.tla \
  --inv=SafetyConjunction --cinit=ConstInit --length=5 --no-deadlock

./scripts/check-tla.sh account
```

## Apalache Symbolic Verification Results (`0.63.0`, Z3 `QF_LIA`)

- Module: [`02_models/L1_SymOrsOrderLifecycle.tla`](02_models/L1_SymOrsOrderLifecycle.tla) (`model_sha256: 2e1d0cf635219970caff0aaa8225e662a55d8194446d9fe1a16d26d2b416a2c9`)
- Inductive Proof (`parallel wall = 4s`, `exit 0`):
  - `base` (`Init => IndInv`): `NoError`, `101` VCs, `3.484s` (wall `4s`)
  - `step` (`IndInv /\ Next => IndInv'`): `NoError`, `101` VCs across `12` transitions, `3.848s` (wall `4s`)
  - `entailment` (`IndInv => SafetyConjunction`): `NoError`, `94` VCs, `3.509s` (wall `4s`)
- Bounded Model Checking (`length = 5`, `--no-deadlock`, `exit 0`):
  - `bmc`: `NoError`, `94` VCs across `12` transitions, `6.429s` (wall `7s`)

## TLC `BoundedMode` Action-Coverage Results

- Run 1 ([`02_models/L1_SymOrsOrderLifecycle.cfg`](02_models/L1_SymOrsOrderLifecycle.cfg), `AllowDryRun = FALSE`): `3,050` states generated, `870` distinct states, `0` left on queue (`01s`)
- Run 2 ([`02_models/L1_SymOrsOrderLifecycle_dryrun.cfg`](02_models/L1_SymOrsOrderLifecycle_dryrun.cfg), `AllowDryRun = TRUE`): `4,492` states generated, `1,382` distinct states, `0` left on queue (`01s`)
- Combined Action Coverage: `12/12` entries nonzero (`100%` action coverage across `2` runs).
