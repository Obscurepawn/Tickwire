# tickwire-risk

The risk kernel computes reserves and limits using checked fixed-point arithmetic.

Check units and the direction of rounding at each currency conversion. Compare thresholds and overflow behavior against the machine-readable golden vectors.

[Crate API](src/lib.rs) · [boundaries](tests/boundaries.rs) · [contract vectors](tests/contract_vectors.rs)

Specifications include [risk core](../../specs/implementation/risk-core.md).

```sh
cargo test --locked -p tickwire-risk --all-targets
```
