# tickwire-strategy-example

The [counting strategy](src/lib.rs) records market callbacks, account reports and whether it has already emitted its order. Exercise both observation-only parameters and a configured limit order after changing its thresholds or state. The [two-worker example](README.md) checks that distinct processes retain independent counts on the same market stream.

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
```
