# tickwire-release

Release commitments describe strategy binaries, input history, verification and approved transitions.

Verify that checkpoint, replay tail and installed candidate refer to the same member and cut. Exercise the old executable across schema changes.

[Crate API](src/lib.rs) · [replay](tests/replay.rs) · [retained installation](tests/retained_installation.rs)

Specifications include [instrument catalog](../../specs/implementation/instrument-catalog.md), [release preflight](../../specs/implementation/release-preflight.md), [strategy rebuild](../../specs/implementation/strategy-rebuild.md), [strategy replay](../../specs/implementation/strategy-replay.md).

```sh
cargo test --locked -p tickwire-release --all-targets
```
