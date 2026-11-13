# Exchange Clock Calibration and Signing Windows

`tickwire-clock::Calibrator` maps remote exchange server timestamps onto conservative `[lower_ns, upper_ns]` intervals anchored to a single suspend-aware local monotonic `ClockDomain` (`nonzero 16-byte boot_id`, `nonzero generation`) and a single venue `Source` (`SpotProduction`, `SpotTest`, `UsdmProduction`, or `UsdmTest`). Built in safe Rust (`#![forbid(unsafe_code)]`), it bounds network asymmetry, server timestamp quantization, and local oscillator drift so outbound signed requests are proven neither ahead of the server nor expired upon arrival.

One `Calibrator` instance owns at most one in-flight `SampleToken`, the latest accepted `Reading` anchor and its `valid_until_ns` deadline, the highest observed local monotonic reading (`last_seen`), the next unused `u64` sample serial (`u64::MAX` is usable once before `next_serial` becomes `None`), and any latched fatal `Error`. Callers supply suspend-aware monotonic nanoseconds ([system-clock.md](system-clock.md)) and drive network I/O externally.

## Interval Math and Drift Bounds

Configuration (`CalibrationConfig`) requires nonzero `max_round_trip_ns`, `max_width_ns`, and `max_age_ns`, `quantization_error_ns >= 1_000_000` (`>= 1 ms`), and `drift_ppb < 1_000_000_000` (`d = drift_ppb / 10^9 < 1`).

Let `S = server_time_ms * 10^6` be the venue timestamp in nanoseconds, `q = quantization_error_ns`, `d = drift_ppb / 10^9`, and `RTT = received.ns - sent_ns`. Assuming local clock rate error stays within `+-d` from request initiation through anchor expiry:

1. **Anchor bounds at receipt (`t = received.ns`)**:
   ```text
   L_0 = max(0, S - q)
   U_0 = S + q + ceil(RTT * (1 + d))
   valid_until_ns = received.ns + max_age_ns
   ```
2. **Outward projection after elapsed age `a = now.ns - sampled_at_ns`**:
   ```text
   L(now) = L_0 + floor(a * (1 - d))
   U(now) = U_0 + ceil(a * (1 + d))
   ```
   Outward rounding (`floor` on lower advance, `div_ceil` in `u128` on upper advance) guarantees `[L(now), U(now)]` strictly encloses true server time across integer truncation boundaries.
3. **Timestamp selection (`sign_time`)**: Rejects `TimeUnit::Microseconds` on USD-M sources (`Invalid`), projects `[L(now), U(now)]` via `read(now)`, computes `midpoint = L(now) + (U(now) - L(now)) / 2`, and rounds down (`midpoint / unit.nanos()`) to produce a private-constructed `SignedTime`.
4. **Pre-send window check (`check_send`)**: Given the original `stamp` (`T = stamp.timestamp * stamp.unit.nanos()` in `u128`), current `now`, `recv_window_ns` (`W`, `0 < W <= 60 s`, exact multiple of `1 ms` for USD-M or `1 us` for Spot), and conservative remaining delivery budget `B = remaining_budget_ns`, requires:
   ```text
   now.ns + B < valid_until_ns           (else Error::Expired)
   T < L(now) + 1_000_000_000            (else Error::Future; Binance 1-second ahead limit)
   U(now) + ceil(B * (1 + d)) <= T + W   (else Error::TooOld; evaluated in u128 to preserve signed difference when T > U)
   ```
   Because `check_send` compares `stamp` against the *current* anchor, a newer calibration sample may validate a request signed under an earlier sample serial.

## Core Invariants

* **Domain and monotonic check before serial allocation (`begin`)**: `begin(now)` checks latched faults, `now.domain == self.domain`, and `now.ns >= last_seen` *before* checking `self.pending.is_some()` (`Error::Busy`) or consuming `next_serial` (`Error::Overflow` if exhausted after `u64::MAX`).
* **Token-first obsolete-reply isolation (`finish` / `cancel`)**: `finish(token, received, server_time_ms)` and `cancel(token)` compare `self.pending.as_ref() == Some(token)` *first*, returning `Error::Stale` immediately on mismatch without observing `received` or mutating `last_seen`, `anchor`, or `fault`—even if the obsolete reply carries malformed timestamps.

## Transitions and Failure Semantics

* **Non-destructive rejection of noisy or expired readings**: After a matching token passes `observe(received)` and clears `pending`, `server_time_ms == 0` (`Error::Invalid`), `RTT > max_round_trip_ns` (`Error::RoundTrip`), or `U_0 - L_0 > max_width_ns` (`Error::Uncertain`) rejects only the candidate sample while leaving any existing `anchor` and its original `valid_until_ns` intact. Likewise, `read(now)` rejects `now.ns >= valid_until_ns` (`Error::Expired`) or projected width `> max_width_ns` (`Error::Uncertain`) without altering `anchor.valid_until_ns`.
* **Irreversible latching on clock integrity faults**: Observing a mismatched `ClockDomain` (`Error::Domain`), regressed monotonic reading (`Error::TimeBackwards`), `u64` arithmetic/serial overflow (`Error::Overflow`), or a disjoint gap `> max_adjustment_ns` between a still-valid projected anchor and a new candidate (`Error::ClockJump`) permanently latches `self.fault`, clears both `pending` and `anchor`, and causes all subsequent calls (`begin`, `read`, `sign_time`, `check_send`) to return `Error::Faulted`.

## Verification and References

```sh
cargo test --locked -p tickwire-clock
./scripts/check-tla.sh
```

Rust unit and integration tests exercise `u128` outward rounding, stale-token isolation, non-destructive noisy-sample rejection, server clock-jump latching, `sign_time`/`check_send` window boundaries, and `u64::MAX` serial/timestamp edges, backed by an exhaustive finite TLA+ calibration model.

* [Calibrator and interval projection implementation](../../crates/tickwire-clock/src/lib.rs)
* [Outward rounding, fault-latching, and window boundary tests](../../crates/tickwire-clock/tests/calibration.rs)
* [Finite TLA+ calibration model (`L0_calibration`)](../../tla/execution_models/clock/L0_calibration/)
* [Native suspend-aware monotonic clock specification](system-clock.md)
* [Private session clock adapter specification](binance-private-clock-adapter.md)
