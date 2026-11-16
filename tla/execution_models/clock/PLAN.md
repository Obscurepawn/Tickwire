# Clock calibration verification plan

The `clock` ladder models single-owner `&mut Calibrator` atomic transitions under an adversary that advances local monotonic time, samples venue server time anywhere inside `[m0, m1]`, and injects stale replies, malformed samples, or server clock jumps. Transport clients, OS clock providers, and TLS belong to separate models (`system_clock` and `binance_private_session`).

| Level | Scope |
| --- | --- |
| `L0_calibration` | Sample token lifecycle, interval uncertainty outward rounding, fault revocation, and exact signed-send window checks using 10-limb base-1000 exact integers (`@TLAPlusOperator` bytecode-accelerated) |
| `L1_sym_calibration` | `Apalache + Z3` (`QF_LIA`) unbounded mathematical induction (`\A k >= 0`) over full `0 .. 2^64 - 1` / `u128` drift cones proving continuous server-truth containment + bounded TLC full action coverage |

Nanosecond, microsecond, and millisecond units match production constants (`1e9`, `1e3`, `1e6`) without rescaling, preserving exact outward-rounding and strict future-bound arithmetic.
