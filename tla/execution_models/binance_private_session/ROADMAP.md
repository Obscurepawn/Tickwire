# Binance private session roadmap

| Level | Status | Evidence |
| --- | --- | --- |
| `L0_session_lifecycle` | PASS | `v2` four configurations, 16 invariants; `Init` + 33 `Next` actions covered across configurations with dedicated post-callback final monotonic sample |
| `L1_transport_scheduling` | Planned | Dual-lane exact I/O budgets, `Pong`/`Close` priority, and frame scheduling |
| `L2_account_barrier` | Planned | Composition with exchange-snapshot account recovery barriers |
