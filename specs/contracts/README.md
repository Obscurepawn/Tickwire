# Machine-Readable Contracts and Golden Vectors

Files in this directory define exact binary layouts, fixed-point rounding vectors, catalog digests, and configuration schemas shared across components and languages. Whenever a wire format, schema field, or risk formula changes in `crates/`, update the corresponding contract and run its independent Python validator before committing.

| Contract Artifact | Normative Boundary Fixed | Independent Validator |
| :--- | :--- | :--- |
| [IPC Wire Layout](ipc-wire-v1.json) & [Golden Vectors](ipc-vectors-v1.json) | Exact byte offsets, alignment, zero-filled reserved fields, type tags, Castagnoli CRC32C checksums, and positive/negative decode cases | `python3 scripts/verify-ipc-contract.py` |
| [Risk Vectors](risk-vectors-v1.json) | Asset scales, Spot/USDⓈ-M margin and fee formulas, collateral haircuts, outward rounding directions, and expected fixed-point outputs | `python3 scripts/verify-risk-vectors.py` |
| [Journal Wire Format](journal-wire-v1.json) | WAL v1 physical header/trailer layout and embedded canonical venue-event version/status tags | `python3 scripts/verify-design-contracts.py` |
| [Instrument Catalog](instrument-catalog-v1.json) | Canonical asset and contract economics, deterministic byte serialization, SHA-256 hash, and semantic digest | `python3 scripts/verify-instrument-catalog.py` |
| [System Config Schema](system-config.schema.json) & [Patch Schema](config-patch.schema.json) | Full deployment configuration, permission-scoped process projections, identities, numeric bounds, and valid patch edits | `python3 scripts/verify-config-contract.py` |
| [Reference Deployment](deployment-reference-v1.json) | Single-host resource envelopes and engineering latency budgets across stages | `python3 scripts/verify-config-contract.py` |

## Running Contract Checks

Execute the full contract verification suite from the repository root using Python 3.11+:

```sh
python3 -m venv /tmp/tickwire-contracts
/tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
```

> **Note on latency budgets:** Latency figures in `deployment-reference-v1.json` represent engineering design budgets. Empirical latency distributions always belong alongside their exact hardware, kernel environment, build profile, and workload in `specs/evidence/` or the relevant [module specification](../INDEX.md).
