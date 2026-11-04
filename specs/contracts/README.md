# Machine-readable contracts

Files in this directory define bytes, units and cross-component limits. Change the relevant vectors and validator alongside the Rust implementation. The [specification index](../INDEX.md) links to each state owner.

| Contract | What it fixes | Verification |
| --- | --- | --- |
| [IPC wire](ipc-wire-v1.json) and [golden vectors](ipc-vectors-v1.json) | Record layout, alignment, reserved bytes, type tags, checksums and accepted or rejected messages | `python3 scripts/verify-ipc-contract.py` |
| [Risk vectors](risk-vectors-v1.json) | Input amounts, units, collateral treatment, rounding and expected fixed-point results | `python3 scripts/verify-risk-vectors.py` |
| [Configuration schema](system-config.schema.json) | Draft 2020-12 schema for full and projected configuration, identities and bounds | `python3 scripts/verify-config-contract.py` |
| [Control API](control-api.openapi.json) | OpenAPI 3.1.1 paths, roles, request/response shapes and status codes | `python3 scripts/verify-design-contracts.py --openapi` |
| [Replication profile](replication-profile-v1.json) | Local/remote commit requirements, retention and failover assumptions for clustered deployments | `python3 scripts/verify-design-contracts.py` |
| [Reference deployment](deployment-reference-v1.json) | Resource and latency budgets for the reference topology | `python3 scripts/verify-design-contracts.py` |
| [Instrument catalog](instrument-catalog-v1.json) | Canonical asset/instrument economics, exact bytes and semantic digest | `python3 scripts/verify-instrument-catalog.py` |

Run the commands from the repository root. Latency values in a deployment profile are engineering budgets. A measured distribution belongs with its machine, workload and build in the evidence directory.
