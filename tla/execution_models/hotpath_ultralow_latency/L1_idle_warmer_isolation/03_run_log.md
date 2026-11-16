# Run log — `L1_IdleWarmerIsolation`

- **Engine**: TLC (`./scripts/check-tla.sh hotpath_ultralow_latency`)
- **Model**: [02_models/L1_IdleWarmerIsolation.tla](02_models/L1_IdleWarmerIsolation.tla)
- **Run 1 (`WarmupInterval = 2`)**: [02_models/L1_IdleWarmerIsolation.cfg](02_models/L1_IdleWarmerIsolation.cfg)
  - States: `151,624` generated, `45,550` distinct, `0` left on queue, depth `18`, wall time `01s`.
  - Verdict: `0` errors (`TypeOK`, `LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsIdlePolicy` all hold).
- **Run 2 (`WarmupInterval = 0`)**: [02_models/L1_IdleWarmerIsolation_Disabled.cfg](02_models/L1_IdleWarmerIsolation_Disabled.cfg)
  - States: `14,110` generated, `3,690` distinct, `0` left on queue, depth `16`, wall time `00s`.
  - Verdict: `0` errors; combined coverage `9/9` entries (`Init` + all `8` `Next` actions) fired with `> 0` generated states.
