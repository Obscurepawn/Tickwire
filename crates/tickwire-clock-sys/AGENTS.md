# tickwire-clock-sys

`tickwire-clock-sys` is the audited native clock boundary (`#![deny(unsafe_op_in_unsafe_fn)]`) that
pairs `std::time::Instant` with the platform's suspend-aware monotonic clock into `SystemSample`
values bound to one externally assigned `ClockDomain`. Supervisor or runtime startup supplies the
`ClockDomain`, I/O loops consume `SystemSample::instant` for kernel deadlines, and `tickwire-clock`
owns exchange-time interval calibration while keeping downstream crates `#![forbid(unsafe_code)]`.

## Invariants and Change Guidelines

* **Audited `clock_gettime` FFI**: Isolate the single `unsafe` call to `libc::clock_gettime` inside
  `native_nanos` in [src/lib.rs](src/lib.rs), using `CLOCK_BOOTTIME` on Linux and
  `CLOCK_MONOTONIC_RAW` on macOS with a stack-initialized `libc::timespec` that never escapes the
  call frame. Non-supported targets return `Error::Unsupported`.
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
