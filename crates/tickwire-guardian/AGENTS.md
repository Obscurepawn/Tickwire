# tickwire-guardian

The guardian handles scoped emergency queries and cancellations on a separately authorized account path.

Keep the operator request, authority evidence and resulting wire operation correlated. Check duplicate emergency requests and expired permissions.

[Crate API](src/lib.rs)

Specifications include [guardian](../../specs/implementation/guardian.md).

```sh
cargo test --locked -p tickwire-guardian --all-targets
```
