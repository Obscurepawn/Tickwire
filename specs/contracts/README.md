# Machine-readable contracts

Files in this directory define bytes, units and cross-component limits. Change the relevant vectors and validator alongside the Rust implementation. The [specification index](../INDEX.md) links to each state owner.

| Contract | What it fixes | Verification |
| --- | --- | --- |
| [IPC wire](ipc-wire-v1.json) and [golden vectors](ipc-vectors-v1.json) | Record layout, alignment, reserved bytes, type tags, checksums and accepted or rejected messages | `python3 scripts/verify-ipc-contract.py` |
| [Risk vectors](risk-vectors-v1.json) | Input amounts, units, collateral treatment, rounding and expected fixed-point results | `python3 scripts/verify-risk-vectors.py` |
| [Configuration schema](system-config.schema.json) | Full and projected configuration, identities and bounds | `python3 scripts/verify-config-contract.py` |
| [Configuration patch](config-patch.schema.json) | Fields and values accepted by a configuration edit | `python3 scripts/verify-config-contract.py` |
| [Journal wire format](journal-wire-v1.json) | Durable record layout and compatibility tags | `python3 scripts/verify-design-contracts.py` |
| [Reference deployment](deployment-reference-v1.json) | Single-host resource limits and latency targets | `python3 scripts/verify-config-contract.py` |
| [Instrument catalog](instrument-catalog-v1.json) | Canonical asset/instrument economics, exact bytes and semantic digest | `python3 scripts/verify-instrument-catalog.py` |

Run the commands from the repository root. Latency values in a deployment profile are engineering budgets. A measured distribution belongs with its machine, workload and build in the evidence directory.
