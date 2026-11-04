# Native monotonic clock

`tickwire_clock_sys::SystemClock::new(domain)` samples the OS before publishing a clock. Each instance keeps that domain for its lifetime. Sampling reads `Instant::now()` and then the platform clock. Linux uses `CLOCK_BOOTTIME`, macOS uses `CLOCK_MONOTONIC_RAW`, and other platforms return `Unsupported`. Check nonnegative seconds, nanoseconds in `0..1_000_000_000`, and checked conversion to u64. The native call writes to an aligned, initialized timespec valid for the duration of the call.

A read or conversion error records a permanent fault. A backward reading on either clock records `Backwards`. Equal readings are valid. Later samples on a faulted instance return `Faulted` without another system call. Lease age and calibration use the returned `MonoTime`. Transport deadlines use the separately sampled Instant.

The Binance client maps native `Backwards` and `Overflow` to their respective core errors. The client retains fatal domain, backward-time, overflow, clock-jump and faulted results. After a successful provider read it still checks its last Instant and passes the monotonic sample through calibration. A later provider success cannot clear either layer's existing fault.

The original native test suite covered seven clock cases, 16 HTTPS-client cases and one real adapter case. The four configurations of the [native owner model](../../tla/execution_models/system_clock/L0_native_owner/05_output.md) explored 3,021, 2,431, 18,714 and two distinct states, covering 14 invariants and 20 Next actions. Rust tests cover u64 conversion limits.
