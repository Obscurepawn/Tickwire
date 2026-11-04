# tickwire-clock-sys

The native clock boundary samples the platform's suspend-aware monotonic clock.

Keep platform calls and conversion checks here. Review the relationship between the OS sample, the assigned clock domain and the Instant passed to I/O deadlines.

[Crate API](src/lib.rs) · [native](tests/native.rs)

Specifications include [system clock](../../specs/implementation/system-clock.md).

```sh
cargo test --locked -p tickwire-clock-sys --all-targets
```
