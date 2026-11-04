# Linux trading lab

The lab runs the real market-data process, strategy worker, account runtime and replica receiver against a local TLS exchange. Start Docker and run these commands from the workspace root.

```sh
./scripts/run-core-chain-linux.sh spot-fill
./scripts/run-core-chain-linux.sh spot-handover release
./scripts/run-core-chain-linux.sh spot-handover-custom release
```

Each run uses a fresh volume and a dedicated set of keys. The launcher records the actual child PID, UID, GID and executable digest before releasing startup. Market data and reports use shared memory, JOIN uses Unix sockets, and both the account and its mirror write file-backed journals.

## What the scenarios verify

* **Spot fill.** The account reconciles before the exchange changes its quote. The strategy buys 0.001 BTC. The final balances are 1.001 BTC and 940 USDT. The lab checks `FeesReconciled`, the subsequent account activation, the order's fee closure, the input WAL prefix, and the ordered records consumed by the replica and strategy.
* **Handover.** The old worker consumes a checkpoint at S, then the launcher starts a shadow successor. After the successor restores S and issues READY, the old instance sends the order and receives its ACK. The lab holds back its fill. At cut C, the old binary replays S to C, the new binary installs C, and a third process probes rollback. Only after Commit, Release and the old child's exit does the late fill enter the account. The original instance remains the creator. The successor manages the order and consumes its report.
* **Custom worker.** `spot-handover-custom` repeats that sequence with the two statically registered [counting workers](../tickwire-strategy-example/README.md). The new schema and probe must preserve the previously consumed action.
* **Market expansion.** `./scripts/run-core-chain-linux.sh spot-market-expansion release` adds an instrument to the running bank, verifies that the consumers have rejoined and completes a handover on the new bank.
* **Declared rebuild.** `./scripts/run-core-chain-linux.sh spot-rebuild-custom release` captures a named input window, reconstructs strategy state in the target process, checks rollback separately and releases the successor.

The fill fixture sends depth every 100 ms. Handover uses one update per second and records the first update after ARM. These periods control event order in the fixture. Use a separate load driver for latency measurements.

## Run without Docker on Linux

```sh
tickwire-lab generate --scenario spot-fill --out /absolute/new-run --bin-dir /absolute/bin
tickwire-lab run --config /absolute/new-run/scenario.json
```

Generate a fresh directory after rebuilding the binaries. Handover also requires `tickwire-lifecycle`. The wrapper copies `workspace-source-sha256.txt`, `deployment-binaries-sha256.txt` and results to `.tools/lab-results/<run-id>/`. The volume retains the journal and captures. A successful scenario writes `run-result.json` after its checks complete. The optional `release` build uses the same coordinator and account deadlines as `dev`.
