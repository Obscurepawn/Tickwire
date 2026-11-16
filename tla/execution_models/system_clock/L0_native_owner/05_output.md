# L0 output

All four bounded configurations verified all 14 safety invariants across `SystemClock::{new, sample}`, native-to-`MonotonicClock` error mapping, and uncalibrated `BinanceTimeClient` `provider -> local -> sample` ordering.

Within the modeled scope: failed initial probes never publish a `SystemClock` owner; `ClockDomain` stays fixed after construction; accepted `Instant` and native readings never regress while permitting equal ticks; native regressions or syscall errors preserve the prior accepted reading and permanently block future `native_nanos` calls; moving `SystemClock` into `BinanceTimeClient` prevents direct owner calls; all five permanent provider error kinds latch `client.fatal` and halt subsequent provider calls; `core.fault` remains distinct from `client.fatal` while blocking valid outputs; and transient provider errors do not latch `client.fatal`.

## Fidelity & coverage

Models 7 functions/methods across `SystemClock`, `Guard`, native error mapping, and `BinanceTimeClient::{local, sample}` plus the unanchored `Calibrator::{observe, read}` subset across `3,021`, `2,431`, `18,714`, and `2` distinct states (`20/20` union `Next` actions with `generated > 0`; see [03_run_log.md](03_run_log.md) and [results.json](04_evidence/results.json)).
