# Test and verification commands

Run commands from the workspace root. Rust tests exercise implementation behavior. Python checks compare machine-readable contracts and fixtures. The Linux process suite starts separate binaries, and the TLA+ suite explores finite state models.

| Task | Command | What to inspect |
| --- | --- | --- |
| Format, test and lint Rust | `./scripts/check-rust.sh` | Workspace targets, doc tests and Clippy |
| Exercise Linux IPC | `./scripts/check-linux.sh` | Real memfd seals, FD access, Unix peer credentials and cross-process JOIN. Requires Docker. |
| Run an account and two generations of a strategy | `./scripts/run-core-chain-linux.sh spot-handover-custom release` | Account journal, binary digests and old/new worker receipts |
| Measure native Bybit and OKX BBO decoding | `cargo bench --locked -p tickwire-md --bench spot_bbo_decode` | Individual-frame p50, p99 and p99.9 from JSON through integer normalization |
| Measure book updates | `cargo bench -p tickwire-book --bench depth_apply` | Release-build latency distribution and input batch |
| Check finite models | `./scripts/check-tla.sh` | Exhausted state queues and nonzero action coverage for each model across its configurations. Requires Java. |

## Contracts

Use Python 3.11 or later. Install the optional schema validators in a local environment for the full OpenAPI check.

```sh
python3 -m venv /tmp/tickwire-design-env
/tmp/tickwire-design-env/bin/python -m pip install -r scripts/requirements-design.txt
/tmp/tickwire-design-env/bin/python scripts/verify-design-contracts.py --openapi
python3 scripts/verify-documentation.py
```

* [Design contracts](verify-design-contracts.py) combines the IPC, risk and configuration checks and cross-checks CPU, memory, storage and latency budgets.
* [IPC vectors](verify-ipc-contract.py) checks field widths, offsets, reserved bytes, encodings, checksums and semantic cases. [Instrument catalogs](verify-instrument-catalog.py) independently computes canonical bytes, SHA-256 and the economic digest.
* [Risk vectors](verify-risk-vectors.py) recalculates expected amounts using decimal arithmetic. [Configuration](verify-config-contract.py) checks reference TOML, projections, schema and cross-field failures.
* [Documentation](verify-documentation.py) checks English prose, links and Markdown structure across the project guides and specifications.

The Rust workspace selects the Raft-compatible compiler through [protoc-raft](protoc-raft). The model runner verifies the pinned TLC 1.7.4 JAR, copies each model and its configurations to a temporary directory, and checks the final coverage report. Run `python3 -m unittest discover -s scripts -p test_tlc_coverage.py` for the [coverage parser](tlc_coverage.py).
