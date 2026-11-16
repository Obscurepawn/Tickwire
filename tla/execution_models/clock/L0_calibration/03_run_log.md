# L0 calibration TLC run log

Module `L0_Calibration.tla` SHA-256 is `cc453c8ae056e4eccbcecc362d88f94c6bbac549ae6c2540e3e12a330d475ffd`. Re-run all four configurations from the workspace root via `./scripts/check-tla.sh clock`, which compiles the bytecode-equivalent `@TLAPlusOperator` Java `Word` override (`scripts/tlc_native_overrides.py`) without modifying `L0_Calibration.tla`.

## Exact-word arithmetic self-check

`04_evidence/int-limit.log` records TLC's native 32-bit `2147483647 + 1` overflow (exit code `75`), motivating the 10-limb base-1000 representation. `04_evidence/word-vectors.py` checks 79 arbitrary-precision arithmetic vectors plus ordering and quantization assertions (`04_evidence/word-final-check.log` and `04_evidence/word-embedding-check.log`).

## Exhaustive model checking

All four configurations (`L0_Calibration_boundary.cfg`, `L0_Calibration_primary.cfg`, `L0_Calibration_rounding.cfg`, `L0_Calibration_usdm.cfg`) pass exhaustive BFS with `0` states left on queue and full `16/16` coverage (`Init` + 15 `Next` actions across line-mapped `Apply` call sites: line 296 `Begin`, line 310 `Read`, line 313 `ReadWrongDomain`, line 314 `Sign`, line 317 `CheckForeign`, plus the remaining named transitions). `boundary` explores `493,130` generated / `64,292` distinct states at depth `12`; earlier multi-process scratch collisions during initial authoring are archived in `initial-runs/`.
