# tickwire-instrument-index

The index translates stable external instrument IDs into dense local slots.

Build and size the lookup structure on the cold path. Verify the complete key on lookup and preserve existing indices when a dynamic catalog grows.

[Crate API](src/lib.rs) · [index](tests/index.rs)

Specifications include [instrument index](../../specs/implementation/instrument-index.md).

```sh
cargo test --locked -p tickwire-instrument-index --all-targets
```

Run the benchmarks [`lookup`](benches/lookup.rs).
