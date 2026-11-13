# Linux Trading Lab (`tickwire-lab`)

`tickwire-lab` exercises Tickwire's complete multi-process trading pipeline on Linux against a deterministic, loopback-only TLS exchange fixture. Unlike unit mocks, the laboratory spawns real OS processes—`tickwire-md`, `tickwire-strategy-worker`, and `tickwire-app`—verifying sealed `memfd` rings, `SCM_RIGHTS` descriptor handshakes, durable WAL commits, TLS order signing, fee reconciliation, and return execution reports end to end.

---

## Run the Containerized `spot-fill` Scenario

With Docker running, execute the full Binance Spot roundtrip from the workspace root:

```sh
./scripts/run-core-chain-linux.sh spot-fill
```

Append `release` (`./scripts/run-core-chain-linux.sh spot-fill release`) to build and measure optimized release binaries.

### Lifecycle and Observable Outputs

1. **Startup & Reconciliation:** The lab starts the local TLS exchange, market-data process, strategy worker, and account runtime. The account reconciles against the venue and delivers its initial baseline report to the worker.
2. **Trigger & Order Execution:** Once the worker confirms receipt of its account baseline, the exchange publishes an eligible BTC quote, prompting the worker to submit a `0.001` BTC limit buy order.
3. **Accounting & Verification:** After validating the signed order request, emitting the private fill, and completing terminal fee reconciliation, the harness writes `run-result.json` and `account-report.json` under `.tools/lab-results/<run-id>/`.

Verify that the output contains:

* `"laboratory_spot_fill_complete"` and `"worker_report_position_verified"`.
* Final reconciled balances of `1.001` BTC and `940` USDT, with all ordered account records consumed by the worker.
* `eligible_quote_to_order_ns` — a single monotonic latency observation measured inside the exchange fixture from the moment the eligible depth update is prepared to the moment the complete signed HTTP order arrives (spanning local TLS, market decoding, SHM ring publication, OS worker scheduling, strategy callback, WAL `fsync`, and outbound TLS transmission).

---

## Run Native Host Binaries Directly

On a Linux host with the pinned Rust toolchain and GNU `binutils` (`strip`), you can run the scenario without Docker. Use a short temporary directory path so Unix domain control sockets fit comfortably within kernel path limits:

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

During `generate` and `run`, the launcher computes SHA-256 digests of the stripped executables, binds each child's expected `PID` and OS credentials, and transfers shared-memory descriptors exclusively over authenticated Unix sockets.
