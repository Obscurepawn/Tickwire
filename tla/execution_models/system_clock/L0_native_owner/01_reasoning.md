# L0 modeling rationale and source mapping

Transcribes `SystemClock` and `BinanceTimeClient` under adversarial fault inputs: syscall errors, invalid `timespec` fields, conversion overflow, and `Instant` / native-clock regressions (`Guard::observe`).

## Call boundaries

Each public `&mut` call executes atomically. `SystemClock::sample` reads `Instant`, invokes `native_nanos`, checks `Guard`, and returns without concurrent re-entry while keeping the two readings distinct. `SystemClock::new` probes `sample` before publishing the owner; once moved into `BinanceTimeClient`, direct owner calls are disabled. In the uncalibrated `BinanceTimeClient` scope modeled here, `local` checks `fatal`, queries the provider, checks `Instant` monotonicity, and invokes `Calibrator::read` (`Uncalibrated` with `signed = None` on success).

## State and transition mapping

| Model state / witness | Production correspondence |
| --- | --- |
| Owner lifecycle (`Unbuilt`, `BuildFailed`, `Direct`, `Moved`) | `Result<SystemClock>` construction boundary and Rust move ownership |
| `native.last`, `native.faulted` | `Guard.last` (updated only on accepted samples) and sticky `Guard.faulted` |
| Native error kinds | Syscall failure, `InvalidTime`, `Overflow`, `Unsupported`, and `Backwards` |
| `client.lastInstant`, `client.fatal` | `BinanceTimeClient.last_instant` and sticky `fatal` |
| `core.last`, `core.fault` | Unanchored `Calibrator.{last_seen, fault}` (distinct from `client.fatal`) |
| `providerCalls`, `nativeReads` | Ghost counters proving zero provider calls after `client.fatal` and zero `native_nanos` attempts after `native.faulted` |

## Bounded configurations

Native and generic primary configurations explore readings `0..2` up to `5` calls; `L0_NativeOwner_long.cfg` expands to readings `0..3` and `7` calls; and `L0_NativeOwner_unsupported.cfg` checks the unsupported-platform construction failure branch.
