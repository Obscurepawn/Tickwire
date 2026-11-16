# L0 state and exact integer representation

`core` tracks `Calibrator.{next_serial, pending, anchor, last_seen, fault}`, preserving exact error-path side effects: `Busy` observes monotonic time first; `Invalid`, `RoundTrip`, and `Uncertain` on `finish` consume `pending`; `Expired` and `Uncertain` on `read` still advance `last_seen`; stale replies and foreign-domain signatures return before `observe`; and `fault` clears `anchor` and `pending` while retaining fault reason and `last_seen`.

`world` tracks independent local monotonic time and exact server truth scaled by `PPB` (`ns * 10^9`) so `1 ppb` drift over `1 ns` steps incurs zero rounding loss. Server jumps increment the environment epoch, revoking truth-containment claims for prior anchors while testing rejection and `ClockJump` fault handling.

Because TLC 2.19 `IntValue` is signed 32-bit whereas `Calibrator` computes `u128` products at `u64` boundaries, `L0_Calibration.tla` represents integers as exact 10-limb base-1000 little-endian words (`0 .. 10^30 - 1`), verified against 79 arbitrary-precision Python test vectors (`word-vectors.py`) before model checking.

`SignedTime` retains its original source, unit, and quantized timestamp `T`; `check_send` re-reads the current calibrated interval without re-picking a midpoint, checking strict future safety now and `recvWindow` upper-bound safety across the delivery budget.

## Bounded configurations

| Config | Source & drift | Local time domain | Call bound |
| --- | --- | --- | --- |
| `primary` | Spot, `0 ppb` | `0, 1 ms, 2 ms, 10 ms, 11 ms` | 6 |
| `rounding` | Spot, `1 ppb` | `0, 1 ns, 1 s, 1 s + 1 ns, 1 s + 2 ns` | 5 |
| `boundary` | Spot, `0 ppb` | `u64::MAX - 10 ms, +1 ns, +1 ms, u64::MAX` | 5 |
| `usdm` | USD-M, `1,000,000 ppb` | `0, 1 ms, 2 ms, 10 ms, 11 ms` | 6 |
