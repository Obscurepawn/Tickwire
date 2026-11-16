# L0 review notes

Native provider fault state (`Guard.faulted`) and outer `BinanceTimeClient` / `Calibrator` fault states (`client.fatal`, `core.fault`) are modeled separately matching production ownership: `core.fault` does not retroactively set `client.fatal`, yet still blocks any successful reading from returning.

`nativeReads` counts `native_nanos` entry attempts (including the syscall-free `Unsupported` branch), and the initial probe inside `SystemClock::new` seeds `native.last` so subsequent direct or moved `BinanceTimeClient` samples obey monotonicity from construction onward.
