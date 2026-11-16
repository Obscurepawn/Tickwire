# Counterexamples and review log

Zero invariant counterexamples occurred in `Calibrator`. TLC's native 32-bit `IntValue` overflow was resolved via 10-limb base-1000 exact words, and initial scratch-directory collisions are archived in `initial-runs/`.

Review confirmed that `Calibrator::new` explicitly accepts nonzero `first_serial`, so the `boundary` initial state (`first_serial = u64::MAX - 1`, `last_seen = None`) is directly constructible in Rust without prior calls. Sub-millisecond quantization outward rounding was verified by 25 Rust unit tests prior to freezing the model; see the [clock calibration specification](../../../../specs/implementation/clock.md).
