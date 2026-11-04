# Spot roundtrip levels

| Level | Contract | Result |
| --- | --- | --- |
| [L0](L0_live_account/05_output.md) | Paired quote, two member queues, cash admission, first TLS byte, REST acknowledgement, private fill, reconciliation and restart | Bounded safety and conditional progress. A second concurrent order can legitimately abort with zero bytes. |
