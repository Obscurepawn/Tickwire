# Native Suspend-Aware Monotonic Clock

`tickwire_clock_sys::SystemClock` pairs standard Rust `Instant` readings (used for local socket and poll deadlines) with suspend-aware operating-system monotonic nanoseconds bound to a single immutable `ClockDomain` (used for lease expiration, market-data age, and [exchange clock calibration](clock.md)). Constructing `SystemClock::new(domain)` performs an immediate OS clock read and validates monotonicity before publishing a usable instance, ensuring an unsupported platform or broken kernel clock fails during startup rather than on the first live request.

`tickwire-clock-sys` isolates `libc::clock_gettime` and hardware cycle-counter instructions (`rdtsc`, `rdtsc_serialized`, and integer `TscCalibration`) behind `#![deny(unsafe_op_in_unsafe_fn)]` and a safe single-owner `Guard`, while `tickwire-transport::latency_tracer` and `tickwire-binance-clock::BinanceTimeClient` layer hot-path stage/spinlock cycle attribution and nonblocking HTTPS time sampling on top in safe Rust (`#![forbid(unsafe_code)]`).

## Platform Sources, Bounds, and Audited `unsafe` Boundary

Each `SystemClock::sample(&mut self)` reads `Instant::now()` first and then queries the platform's native monotonic clock:

| Target OS | Native Clock ID | Semantics & Validation |
| --- | --- | --- |
| **Linux** | `libc::CLOCK_BOOTTIME` | Monotonic nanosecond counter that continues advancing across system suspend/hibernate. |
| **macOS** | `libc::CLOCK_MONOTONIC_RAW` | Unadjusted hardware-driven monotonic nanosecond counter. |
| **Other platforms** | None | Returns `Error::Unsupported` immediately without FFI calls. |

The audited `unsafe` block passes a stack-allocated, zero-initialized, aligned `libc::timespec { tv_sec: 0, tv_nsec: 0 }` exclusively borrowed for the duration of `libc::clock_gettime` (which retains no pointer after returning). `decode_timespec` widens `tv_sec` and `tv_nsec` to signed `i128` *before* any unsigned cast:

* Negative `tv_sec` or `tv_nsec` outside `0..1_000_000_000` fails with `Error::InvalidTime`.
* `u64::try_from(tv_sec) * 1_000_000_000 + tv_nsec` uses checked arithmetic up to `u64::MAX` (`18,446,744,073s 709,551,615ns`), failing with `Error::Overflow` on `tv_sec * 10^9 + tv_nsec > u64::MAX`.

For ultra-low-overhead hot-path latency tracing and spinlock contention attribution without `clock_gettime` vDSO/syscall cost, `tickwire-clock-sys` also exposes inline hardware counter reads and cold-startup integer calibration:

| Primitive | Architecture Implementation | Semantics & Arithmetic |
| --- | --- | --- |
| `rdtsc() -> u64` | `core::arch::x86_64::_rdtsc()` on `x86_64`; `mrs cntvct_el0` on `aarch64`; `Instant`-anchored monotonic nanoseconds elsewhere | Unserialized single-instruction cycle counter read (`#[inline(always)]`) for hot-path `LatencyTracer` marks and spinlock wait deltas. |
| `rdtsc_serialized() -> u64` | `_mm_lfence()` + `_rdtsc()` on `x86_64`; `isb; mrs cntvct_el0` on `aarch64`; `rdtsc()` fallback elsewhere | Dispatch-serialized counter read for cold-startup calibration boundaries and microbenchmark brackets. |
| `TscCalibration` | `cycles_per_ms: u64` (`>= 1_000`) | Cold-startup `calibrate(Duration)` / `calibrate_default()` measures elapsed cycles over `Instant` nanoseconds and converts `cycles <-> ns/us` in exact `u128` integer arithmetic (zero floating-point operations). |

## Core Invariants

* **Startup validation before publication**: `SystemClock::new(domain)` binds `domain` immutably for the instance's lifetime and executes a full `sample()` before returning `Ok(Self)`, guaranteeing no caller can hold an unverified or unsupported native clock.
* **Dual-clock non-regression**: `Guard` tracks the latest `(Instant, u64)` pair; equal consecutive readings on either clock are valid, whereas a backward step on *either* `Instant` or native nanoseconds—even when the companion clock advances—is rejected immediately as `Error::Backwards`.
* **Audited FFI memory safety**: `native_nanos` passes an initialized, stack-local `&mut libc::timespec` valid for the synchronous `clock_gettime` call and validates signed bounds in `i128` before any unsigned conversion.

## Transitions and Failure Semantics

Any `clock_gettime` OS error (`Error::System`), out-of-range `timespec` (`Error::InvalidTime`), `u64` nanosecond overflow (`Error::Overflow`), unsupported OS (`Error::Unsupported`), or clock regression (`Error::Backwards`) latches `guard.faulted = true`. Once faulted, every subsequent `SystemClock::sample()` returns `Error::Faulted` immediately **without making another system call**.

In `tickwire-binance-clock`, `MonotonicClock for SystemClock` maps native `Backwards` to `tickwire_clock::Error::TimeBackwards`, `Overflow` to `tickwire_clock::Error::Overflow`, and all other native failures to `tickwire_clock::Error::Faulted`. `BinanceTimeClient` independently latches fatal `Domain`, `TimeBackwards`, `Overflow`, `ClockJump`, and `Faulted` states and checks its own last observed `Instant` alongside `Calibrator` monotonicity after every provider read—ensuring a later successful provider sample can never clear an existing fault in either layer.

## Verification and References

```sh
cargo test --locked -p tickwire-clock-sys
cargo test --locked -p tickwire-binance-clock
./scripts/check-tla.sh
```

The Rust test suites verify exact `u64::MAX` `timespec` conversion boundaries, rejection of negative/out-of-range `timespec` fields without unsigned wraparound, permanent latching across `Instant` and native-nanosecond rollbacks (7 native clock cases), HTTPS calibration client fault retention (16 client cases), and real `SystemClock` integration. In addition, four TLC configurations of the finite `L0_native_owner` TLA+ model explored `3,021`, `2,431`, `18,714`, and `2` distinct states with zero violations across 14 safety invariants and 20 `Next` actions.

* [Native `SystemClock` and `decode_timespec` implementation](../../crates/tickwire-clock-sys/src/lib.rs)
* [Native OS clock integration tests](../../crates/tickwire-clock-sys/tests/native.rs)
* [`BinanceTimeClient` and `MonotonicClock` mapping](../../crates/tickwire-binance-clock/src/lib.rs)
* [HTTPS calibration client fault-retention tests](../../crates/tickwire-binance-clock/tests/https.rs)
* [`SystemClock`-backed client tests](../../crates/tickwire-binance-clock/tests/system_clock.rs)
* [Finite TLA+ native clock owner results (`L0_native_owner`)](../../tla/execution_models/system_clock/L0_native_owner/05_output.md)
* [Exchange clock calibration specification](clock.md)
