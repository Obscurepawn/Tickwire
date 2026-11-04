# Linux trading lab

The spot-fill scenario runs a Binance market-data process, a strategy worker and an account against a local TLS exchange. From the workspace root, start Docker and run the scenario.

```sh
./scripts/run-core-chain-linux.sh spot-fill
```

Once the account has reconciled and the worker has consumed its first account report, the exchange changes its quote. The strategy buys 0.001 BTC. Inspect `run-result.json` and `account-report.json` under `.tools/lab-results/<run-id>/`. Look for `laboratory_spot_fill_complete` and `worker_report_position_verified`. The account finishes with 1.001 BTC and 940 USDT. The lab checks fee reconciliation, the account journal and the worker's ordered reports. The field `eligible_quote_to_order_ns` measures from the first eligible price update at the fixture to receipt of the signed order at the same fixture. It includes local network transport, process scheduling and durable admission. Each successful run produces one observation. Append `release` to use optimized binaries.

## Run native binaries

With the pinned Rust toolchain and GNU binutils on Linux, build the four binaries and give the run its own short socket path. Run these commands from the workspace root.

```sh
cargo build --locked -p tickwire-lab -p tickwire-app -p tickwire-md -p tickwire-strategy-sdk --bins
run_dir=$(mktemp -d /tmp/tw-lab.XXXXXX)
mkdir "$run_dir/bin"
for name in tickwire-lab tickwire-app tickwire-md tickwire-strategy-worker; do
    strip --strip-debug -o "$run_dir/bin/$name" "target/debug/$name"
done
"$run_dir/bin/tickwire-lab" generate --scenario spot-fill --out "$run_dir/run" --bin-dir "$run_dir/bin"
"$run_dir/bin/tickwire-lab" run --config "$run_dir/run/scenario.json"
```

The launcher records each child's PID, credentials and executable digest before startup. Workers receive their shared-memory descriptors over authenticated Unix sockets.
