# tickwire-clock-sys

`tickwire-clock-sys` is the audited native clock boundary (`#![deny(unsafe_op_in_unsafe_fn)]`) that
pairs `std::time::Instant` with the platform's suspend-aware monotonic clock into `SystemSample`
values bound to one externally assigned `ClockDomain`, and provides inline hardware cycle-counter
reads (`rdtsc`, `rdtsc_serialized`) plus exact integer `TscCalibration` for zero-overhead hot-path
latency tracing. Supervisor or runtime startup supplies the `ClockDomain` and calibrates TSC rate
off the hot path, I/O loops consume `SystemSample::instant` for kernel deadlines, `LatencyTracer`
consumes `rdtsc()`, and `tickwire-clock` owns exchange-time interval calibration while keeping
downstream crates `#![forbid(unsafe_code)]`.

## Invariants and Change Guidelines

* **Audited `clock_gettime` and hardware counter FFI/intrinsics**: Isolate `unsafe` calls to
  `libc::clock_gettime` (`native_nanos`, using `CLOCK_BOOTTIME` on Linux and `CLOCK_MONOTONIC_RAW`
  on macOS with a stack-initialized `libc::timespec`) and architecture cycle-counter intrinsics/asm
  (`_rdtsc` / `_mm_lfence` on `x86_64`, `cntvct_el0` on `aarch64`) strictly inside
  [src/lib.rs](src/lib.rs) with documented `// SAFETY:` comments. Non-supported targets return
  `Error::Unsupported` from `SystemClock::new` and fall back to `Instant`-anchored monotonic
  nanoseconds in `rdtsc()`.
* **Exact integer TSC calibration**: Keep `TscCalibration` (`cycles_per_ms >= 1_000`) in `u128`
  checked/saturating integer arithmetic—never introduce floating-point math in cycle/nanosecond
  conversions.
* **Checked `timespec` decoding**: Keep `decode_timespec` strict in `i128`/`u64` checked
  arithmetic—reject negative `tv_sec` or `tv_nsec` outside `0..1_000_000_000` with
  `Error::InvalidTime`, and reject `u64` nanosecond wraparound with `Error::Overflow`.
* **Irreversible fault latching**: `SystemClock::new` takes an immediate initial sample at
  construction, and `Guard::observe` requires both `Instant` and native nanoseconds to be
  non-decreasing across calls. Any syscall failure, invalid `timespec`, overflow, or regression
  permanently sets `Guard::faulted` so subsequent `sample()` calls return `Error::Faulted`.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [tests/native.rs](tests/native.rs)
* **Specifications and models**: [system-clock.md](../../specs/implementation/system-clock.md) ·
  [clock.md](../../specs/implementation/clock.md) ·
  [system_clock](../../tla/execution_models/system_clock/ROADMAP.md)

```sh
cargo test --locked -p tickwire-clock-sys --all-targets
./scripts/check-tla.sh system_clock
```
