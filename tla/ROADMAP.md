# Finite models

Each family links its configurations to source, assumptions and recorded checks. Run one family from the repository root, for example `./scripts/check-tla.sh submission`.

| Trading boundary | Model families |
| --- | --- |
| Order intake and submission | [Submission](execution_models/submission/ROADMAP.md) checks local commit receipts, loss of a receipt and frozen restart. Its economic reducer is tied to an earlier source revision. [Ordered abort](execution_models/mock_abort/ROADMAP.md) follows unsent aborts through the current mock journal. |
| Execution and cash | [Account](execution_models/account/ROADMAP.md) covers reservations, late fills and fees. [Account gateway](execution_models/account_gateway/ROADMAP.md) combines the account, local WAL and permission at first write. |
| Durable storage | [Physical journal](execution_models/physical_journal/ROADMAP.md) covers batch sync and torn writes. [Account journal](execution_models/account_journal/ROADMAP.md), [business journal](execution_models/business_journal/ROADMAP.md) and [recovery closure](execution_models/recovery_closure/ROADMAP.md) track account records and the objects they depend on. |
| Network output | [Gateway](execution_models/gateway/ROADMAP.md) follows a durable command into its transport. [Transport](execution_models/transport/ROADMAP.md) distinguishes the first accepted application byte, partial sends, zero-byte termination and TLS flush. |
| Market data and private input | [Order book](execution_models/book_state/ROADMAP.md) checks sequence rules and depth coverage. [Binance depth synchronization](execution_models/binance_depth_sync/ROADMAP.md) covers the snapshot bridge and session generations. [Binance private session](execution_models/binance_private_session/ROADMAP.md) tracks private connection state. |
| Local process communication | [Broadcast ring](execution_models/broadcast_ring/ROADMAP.md), [JOIN](execution_models/join_owner/ROADMAP.md) and [control requests](execution_models/control_requests/ROADMAP.md) cover publication, reader startup, session identity and durable request results. |
| Time | [Clock](execution_models/clock/ROADMAP.md) and [system clock](execution_models/system_clock/ROADMAP.md) model sampling, time-domain conversion and validity bounds. |

Use the per-family run records to identify which configurations terminated and which source revision they apply to. The earlier inventory records the original stages and recovery-metadata changes. [Historical inventory](https://github.com/Obscurepawn/Tickwire/blob/dc50649ebdcada26f7740181c16139cd3b32064a/tla/ROADMAP.md)
