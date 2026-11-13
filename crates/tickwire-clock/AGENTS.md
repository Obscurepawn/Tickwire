# tickwire-clock

`tickwire-clock` (`#![forbid(unsafe_code)]`) owns `ClockDomain`, `MonoTime`, and the single-owner
`Calibrator` state machine that projects exchange server-time samples onto conservative local
monotonic intervals (`Reading`) and issues signed request timestamps (`SignedTime`). Native OS clock
sampling belongs in `tickwire-clock-sys`, and venue transport round-trips belong in exchange session
adapters; callers drive `Calibrator` with explicit `MonoTime` samples and server millisecond
replies.

## Invariants and Change Guidelines

* **Domain and monotonicity latching**: Every state transition in [src/lib.rs](src/lib.rs)
  (`observe`) validates non-zero `ClockDomain` (`boot_id` and `generation`) and non-decreasing
  `MonoTime`. Any domain mismatch, backward timestamp, arithmetic overflow, or excessive `ClockJump`
  permanently latches `Calibrator` into `CalibrationStatus::Faulted` and clears active anchors.
* **Stale sample isolation**: `Calibrator::finish` and `Calibrator::cancel` must verify
  `pending == Some(*token)` before inspecting `received` or `server_time_ms` so delayed or malformed
  replies from superseded probes cannot mutate or fault active calibration state.
* **Conservative drift projection**: Keep `project` and `scale_up` in [src/lib.rs](src/lib.rs)
  asymmetric in `u128` integer math—advancing `lower_ns` by `PPB - drift_ppb` (floor) and `upper_ns`
  by `PPB + drift_ppb` (`div_ceil`)—and enforce `upper_ns - lower_ns <= max_width_ns` plus
  `now.ns < valid_until_ns` on every `read`.
* **Send-window admissibility**: `sign_time` rejects `TimeUnit::Microseconds` for USD-M sources
  (`UsdmProduction`, `UsdmTest`), constructs `SignedTime` privately so `check_send` recovers the
  exact encoded timestamp, and verifies in `u128` arithmetic that the stamp neither leads
  `lower_ns + 1s` (`Error::Future`) nor trails `upper_ns` projected across `remaining_budget_ns`
  (`Error::TooOld`).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [tests/calibration.rs](tests/calibration.rs)
* **Specifications and models**: [clock.md](../../specs/implementation/clock.md) ·
  [system-clock.md](../../specs/implementation/system-clock.md) ·
  [clock](../../tla/execution_models/clock/ROADMAP.md) ·
  [system_clock](../../tla/execution_models/system_clock/ROADMAP.md)

```sh
cargo test --locked -p tickwire-clock --all-targets
./scripts/check-tla.sh clock
```
