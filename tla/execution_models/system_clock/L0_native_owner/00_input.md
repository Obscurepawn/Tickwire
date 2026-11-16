# L0 input

Verifies whether a faulted `SystemClock` owner can ever deliver another reading and whether `BinanceTimeClient` permanently latches fatal provider clock faults.

| Source | Modeled semantics |
| --- | --- |
| `crates/tickwire-clock-sys/src/lib.rs:33-56` | `Guard` retains the last accepted `(Instant, native)` pair; regression on either reading latches permanent `faulted = true`, while equal ticks are permitted |
| `crates/tickwire-clock-sys/src/lib.rs:57-91` | `SystemClock::new` probes once before returning the owner, fixes `ClockDomain`, rejects immediately once `faulted`, and samples `Instant` before `native_nanos` |
| `crates/tickwire-clock-sys/src/lib.rs:92-123` | `timespec` field validation, checked multiply-add, syscall failure, and unsupported-platform branch |
| `crates/tickwire-binance-clock/src/lib.rs:41-55` | Native error mapping: `Backwards -> TimeBackwards`, `Overflow -> Overflow`, remaining errors -> `Faulted` |
| `crates/tickwire-binance-clock/src/client.rs:33-107` | Validated single-owner construction, fatal provider error latching, `Instant` regression check, and calibrator read |
| `crates/tickwire-binance-clock/src/client.rs:378-388` | Public `sample` calls `local` then checks signature availability (`signed = None` when uncalibrated) |
| `crates/tickwire-clock/src/lib.rs:200-211` and `crates/tickwire-clock/src/lib.rs:295-299` | Domain/monotonic order check and `Uncalibrated` return when no calibration anchor exists |

Callers supply a fixed `ClockDomain`. If the initial probe inside `SystemClock::new` fails, `new` returns `Err` without publishing a callable `SystemClock`. Once moved into `BinanceTimeClient`, only `BinanceTimeClient` accesses the provider. Configurations cover both the native `SystemClock` provider and a generic `MonotonicClock` provider exercising all five permanent error kinds (`Domain`, `TimeBackwards`, `Overflow`, `ClockJump`, `Faulted`) alongside transient errors; see the [system clock specification](../../../../specs/implementation/system-clock.md).
