# Binance private session ladder

Scope covers single-owner connection management, authentication/subscription, `listenKey` keepalive renewal, and explicit close in `tickwire-binance-private`. Calls on `&mut PrivateClient` are serialized; first-accept gate callbacks execute synchronously and may advance monotonic time or reject delivery budget without re-entering the owner. The network may disconnect, drop replies, deliver events ahead of replies, or deliver stale or premature frames.

| Level | Scope increment | Status |
| --- | --- | --- |
| `L0_session_lifecycle` | Auth and `listenKey` lifecycle, first-accept gate and final monotonic sample, request correlation, gap and prior-epoch draining | PASS |
| `L1_transport_scheduling` | Exact dual-lane I/O budgets, `Pong`/`Close` priority, and frame-parse scheduling | Planned |
| `L2_account_barrier` | Composition with exchange-snapshot account recovery barriers | Planned |

L0 fixes one account and credential set without re-proving cryptographic signing, TLS, clock-interval arithmetic, or account ledger math. `ClockDomain`, JSON parsing, and socket framing are covered by crate regression suites.
