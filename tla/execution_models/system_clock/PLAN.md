# System clock formal verification ladder

Verifies construction, monotonic sampling, and sticky fault latching in `SystemClock` (`tickwire-clock-sys`) composed with provider error classification and uncalibrated local sampling in `BinanceTimeClient` (`tickwire-binance-clock`). Each instance is owned exclusively (`&mut self`); once moved into `BinanceTimeClient`, callers can no longer invoke `SystemClock` directly.

| Level | Scope | Status |
| --- | --- | --- |
| `L0_native_owner` | Construction probe, fixed `ClockDomain`, `Instant` / native-clock regression guards, permanent fault latching, and provider error classification | PASS, independent review complete |
| Follow-up: `TimeClient` network L0 | HTTPS sample tokens, first-accept, response correlation, timeouts, and calibration handoff | Planned |
| Follow-up: Private-stream clock adapter | Covered in `binance_private_session/L0_session_lifecycle` (`v2`) | Verified |

L0 models clock readings over bounded non-negative integers capturing ordering relationships; `timespec` field range checks and `u64` arithmetic bounds are covered by Rust unit tests, and calibration interval arithmetic is verified in `clock/L0_calibration` and `clock/L1_sym_calibration`.
