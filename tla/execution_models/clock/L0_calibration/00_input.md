# L0 input and scope

Targets `crates/tickwire-clock/src/lib.rs` (paired with `crates/tickwire-clock/tests/calibration.rs` and the [clock calibration specification](../../../../specs/implementation/clock.md)).

Source boundaries: construction and state `crates/tickwire-clock/src/lib.rs:144-212`; sample begin/finish/cancel `crates/tickwire-clock/src/lib.rs:214-293`; interval read and `sign_time` `crates/tickwire-clock/src/lib.rs:295-326`; `check_send` `crates/tickwire-clock/src/lib.rs:328-372`; status and integer uncertainty formulas `crates/tickwire-clock/src/lib.rs:374-409`.

Public `&mut Calibrator` calls execute atomically without interleaving. Venue source, environment, and `ClockDomain` are fixed per instance, while stale tokens or foreign `SignedTime` values may be passed in as adversarial inputs. Permanent faults are sticky on an instance; consumed sample tokens are never reused; and previously issued `SignedTime` values can be re-checked across normal calibration refreshes against their original quantized timestamp.

Hidden server truth is quantized to milliseconds at an arbitrary hidden sample instant `t_s \in [m0, m1]` and then evolves at minimum, neutral, or maximum drift within the configured rate cone (`0`, `1 ppb`, or `1,000,000 ppb`) at real nanosecond scale (`SECOND = PPB = 10^9`, `ms = 10^6`, `us = 10^3`). `Calibrator::new` accepts caller-supplied nonzero `first_serial` (`crates/tickwire-clock/src/lib.rs:166-190`), allowing the `boundary` configuration to start directly at `first_serial = u64::MAX - 1` with `last_seen = None`.
