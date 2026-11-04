# tickwire-clock

Clock domains map an exchange-time interval onto a monotonic timeline.

Keep units explicit and preserve interval bounds during conversion. Test stale calibration, backward samples, excessive uncertainty and readings taken in different boot generations.

[Crate API](src/lib.rs) · [calibration](tests/calibration.rs)

Specifications include [clock](../../specs/implementation/clock.md).

```sh
cargo test --locked -p tickwire-clock --all-targets
```
