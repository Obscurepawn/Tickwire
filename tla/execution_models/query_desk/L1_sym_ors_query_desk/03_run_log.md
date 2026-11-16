# L1 Symbolic 64-Bit ORS QueryDesk Run Log

## Verification Commands

```sh
/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh inductive \
  tla/execution_models/query_desk/L1_sym_ors_query_desk/02_models/L1_SymOrsQueryDesk.tla \
  --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit

/home/obscure/.gemini/config/skills/vibe-tla/scripts/run_apalache.sh bmc \
  tla/execution_models/query_desk/L1_sym_ors_query_desk/02_models/L1_SymOrsQueryDesk.tla \
  --inv=SafetyConjunction --cinit=ConstInit --length=5 --no-deadlock

./scripts/check-tla.sh query_desk
```

## Apalache Symbolic Verification Results (`0.63.0`, Z3 `QF_LIA`)

- Module: [`02_models/L1_SymOrsQueryDesk.tla`](02_models/L1_SymOrsQueryDesk.tla) (`model_sha256: 0085bedf915e2af2547a447031e646536830d444515375643bb225afce0a5dcc`)
- Inductive Proof (`parallel wall = 4s`, `exit 0`):
  - `base` (`Init => IndInv`): `NoError`, `72` VCs, `2.578s` (wall `3s`)
  - `step` (`IndInv /\ Next => IndInv'`): `NoError`, `72` VCs across `16` transitions, `2.954s` (wall `4s`)
  - `entailment` (`IndInv => SafetyConjunction`): `NoError`, `60` VCs, `2.611s` (wall `3s`)
- Bounded Model Checking (`length = 5`, `--no-deadlock`, `exit 0`):
  - `bmc`: `NoError`, `60` VCs across `16` transitions, `3.514s` (wall `4s`)

## TLC `BoundedMode` Action-Coverage Results

- Run 1 ([`02_models/L1_SymOrsQueryDesk.cfg`](02_models/L1_SymOrsQueryDesk.cfg), `OptInMaxCap = FALSE`): `33,206` states generated, `6,069` distinct states, `0` left on queue (`02s`)
- Run 2 ([`02_models/L1_SymOrsQueryDesk_bounded.cfg`](02_models/L1_SymOrsQueryDesk_bounded.cfg), `OptInMaxCap = TRUE`): `27,013` states generated, `4,669` distinct states, `0` left on queue (`02s`)
- Combined Action Coverage: `15/15` entries nonzero (`100%` action coverage across `2` runs).
