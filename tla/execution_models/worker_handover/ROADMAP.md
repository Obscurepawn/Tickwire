# Verification record

| Level | Source | Result |
| --- | --- | --- |
| [L0 market consumer](L0_market_consumer/05_output.md) | `9b2ed63` | Two exhaustive safety runs. A separate seven-state trace and a native socket regression confirm rejection of a valid receipt by the host. |
| [L1 receipt time](L1_receipt_time/05_output.md) | Per-frame host clock | Validated fix. The same sample gates the envelope and the receipt after the read. |
