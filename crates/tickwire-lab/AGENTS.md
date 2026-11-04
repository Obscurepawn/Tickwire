# tickwire-lab

The [spot-fill fixture](src/fixture.rs) supplies Binance market data and fills over local TLS. The launcher records the worker and account process identities. After changing the fixture or event order, run the [Linux scenario](README.md) and check the fee, final balances and worker report position in the output.

```sh
cargo test --locked -p tickwire-lab --all-targets
./scripts/run-core-chain-linux.sh spot-fill
```
