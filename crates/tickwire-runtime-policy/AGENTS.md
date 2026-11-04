# tickwire-runtime-policy

Policy grants combine execution ownership with market freshness, account reconciliation and current limits.

Bind the decision to the requesting strategy, account, venue and epoch. Check expiry and a private-stream gap both at preparation and before the first output.

[Crate API](src/lib.rs)

Specifications include [runtime policy](../../specs/implementation/runtime-policy.md).

```sh
cargo test --locked -p tickwire-runtime-policy --all-targets
```
