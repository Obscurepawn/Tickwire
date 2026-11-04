# tickwire-config-service

The service owns deterministic control state, configuration plans and operator-facing responses.

Bind a plan to the read revision and qualified inputs that produced it. Verify the staged and applied outcomes separately when wiring an API operation.

[Crate API](src/lib.rs) · [account facts](tests/account_facts.rs) · [bootstrap](tests/bootstrap.rs) · [control api](tests/control_api.rs) · [control tls](tests/control_tls.rs) · [credential facts](tests/credential_facts.rs)

Specifications include [config archive sources](../../specs/implementation/config-archive-sources.md), [config control state](../../specs/implementation/config-control-state.md), [config credential facts](../../specs/implementation/config-credential-facts.md), [config desired read](../../specs/implementation/config-desired-read.md), [config plan archive](../../specs/implementation/config-plan-archive.md), [config planning](../../specs/implementation/config-planning.md), [config runtime facts](../../specs/implementation/config-runtime-facts.md), [config server](../../specs/implementation/config-server.md), [instrument catalog](../../specs/implementation/instrument-catalog.md).

```sh
cargo test --locked -p tickwire-config-service --all-targets
```
