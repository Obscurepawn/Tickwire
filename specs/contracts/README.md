# Machine-Readable Contracts and Golden Vectors

Files in this directory define exact binary layouts, fixed-point rounding vectors, catalog digests, and static configuration schemas shared across components and languages. Whenever a wire format, schema field, or risk formula changes in `crates/`, update the corresponding contract and run its independent Python validator before committing.

| Contract Artifact | Normative Boundary Fixed | Independent Validator |
| :--- | :--- | :--- |
| [IPC Wire Layout](ipc-wire-v1.json) & [Golden Vectors](ipc-vectors-v1.json) | Exact ABI 1.0 byte offsets and alignment across `48` fixed layouts, `37` data message IDs, and `38` control opcodes, inline `1024B` `IntentRecord`, `512B` `ReportRecord`, and `512B` `MarketRecord` slots, DOP logon frames (`DopLogin` / `DopEcho` / `OrsSessions`), zero-filled reserved fields, Castagnoli CRC32C checksums, and `86` byte vectors / `32` semantic cases | `/tmp/tickwire-contracts/bin/python scripts/verify-ipc-contract.py` |
| [Risk Vectors](risk-vectors-v1.json) | Asset scales, Spot/USDⓈ-M margin and fee formulas, collateral haircuts, outward rounding directions, and expected fixed-point outputs (`21` cases, `131` expected fields) | `/tmp/tickwire-contracts/bin/python scripts/verify-risk-vectors.py` |
| [Instrument Catalog](instrument-catalog-v1.json) | Canonical asset and contract economics, deterministic byte serialization, SHA-256 hash, and semantic digest (`3` catalog vectors, `22` rehashed rejection checks) | `/tmp/tickwire-contracts/bin/python scripts/verify-instrument-catalog.py` |
| [System Config Schema](system-config.schema.json) | Stateless 3-process deployment schema, permission-scoped process projections, hot vs. `housekeeping_cpus` CPU isolation (`0..4095`), multi-UID `enabled_accounts` (`account_ref = 1..255`), `order_manager`, and static `rms` limits (`1s`/`5s`/`10s`) | `/tmp/tickwire-contracts/bin/python scripts/verify-config-contract.py` |
| [Reference Deployment](deployment-reference-v1.json) | Single-host stateless resource envelopes (`5,910,888,448` bytes committed SHM in `config/reference.toml` within the `8,589,934,592`-byte `resource_limit_bytes` ceiling, `16` sample / `32` max active strategies) and engineering latency budgets across stages | `/tmp/tickwire-contracts/bin/python scripts/verify-config-contract.py` |

## Running Contract Checks

Execute the full contract verification suite from the repository root using Python 3.11+:

```sh
python3 -m venv /tmp/tickwire-contracts
/tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
```

> **Note on latency budgets:** Latency figures in `deployment-reference-v1.json` represent engineering design budgets. Empirical latency distributions always belong alongside their exact hardware, kernel environment, build profile, and workload in `specs/evidence/` or the relevant [module specification](../INDEX.md).
