# Run Log — `L0_SharedOrsLimiter`

## Commands
- `/home/obscure/.gemini/config/skills/vibe-tla/scripts/anchors.sh check tla execution_models/request_budget/L0_shared_ors_limiter/` -> `1099 anchor line(s) across 1 lock(s); 0 changed`.
- `./scripts/check-tla.sh request_budget` -> `PASS` (`0` states left on queue, `14/14` (`100%`) nonzero action coverage across `L0_SharedOrsLimiter.cfg` and `L0_SharedOrsLimiter_multi_ip.cfg`).

## Results
- `L0_SharedOrsLimiter.cfg` (`AllowAccountFreeze = TRUE`, `SharedIpCap = 2`): `1,248,895` states generated, `93,324` distinct states, depth `14`, `0` states left on queue.
- `L0_SharedOrsLimiter_multi_ip.cfg` (`AllowAccountFreeze = FALSE`, `SharedIpCap = 1`): `84,313` states generated, `6,786` distinct states, depth `11`, `0` states left on queue.
- Combined: `1,333,208` states generated, `100,110` distinct states, `14/14` (`100%`) nonzero action coverage, `model_sha256 = 6bb78d57060de26d92f12505fd6d5612243f3d1679041efd5852e7ff501057dc`.
