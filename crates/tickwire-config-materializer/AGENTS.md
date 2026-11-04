# tickwire-config-materializer

The materializer joins file-store state with local journal observations to install an effective configuration.

Follow the revision and digest into both receipts. Failure during the second write must leave a state the next startup can inspect and resume.

[Crate API](src/lib.rs) · [linux](tests/linux.rs) · [sequence](tests/sequence.rs)

Specifications include [config materializer](../../specs/implementation/config-materializer.md).

```sh
cargo test --locked -p tickwire-config-materializer --all-targets
```
