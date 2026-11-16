# Modeling reasoning — `L1_IdleWarmerIsolation`

## Scope and state owners
1. **Stateless ORS live session vs. scratch warmer isolation (`crates/tickwire-runtime/src/session.rs:663-715`, `crates/tickwire-runtime/src/ors.rs:2360-2380`)**:
   - Live ORS state (`live_balance`, `live_open_holds`, `live_stp_resting`, `live_seq_floor`, `live_shm_published`, `live_rate_tokens`, `in_flight_orders`) advances exclusively on live `LiveOrderAdmitAndSend` and `LiveVenueAck` transitions, paired with shadow reference variables (`shadow_*`) verifying zero side effects across `WarmHotPath`.
   - `WarmHotPath` exercises `BinanceHotPathWarmer` (`crates/tickwire-binance-execution/src/warmup.rs:193-468`) and `UtaHotPathWarmer` (`crates/tickwire-spot-execution/src/warmup.rs:201-425`) over stack scratch structures and `AccountSession` `dry_run = true` short-circuit (`crates/tickwire-runtime/src/session.rs:663-690`).
2. **Offline loopback `TlsLoopbackWarmer` vs. live `rustls::ClientConnection` (`crates/tickwire-transport/src/warmup.rs:219-422`, `crates/tickwire-transport/src/connection.rs:249-282`)**:
   - Models independent TLS 1.3 AEAD write/read record sequence numbers on the live venue socket (`live_tls_client_write_seq == live_tls_venue_read_seq`) and on the isolated in-memory `dummy_client <-> dummy_server` pair (`dummy_client_write_seq == dummy_server_read_seq`, `dummy_server_write_seq == dummy_client_read_seq`), plus an explicit contrast guard (`DemonstrateForbiddenLiveTlsDiscardContrast`) proving why encrypting and discarding ciphertext on a live `rustls::ClientConnection` causes immediate `bad_record_mac` nonce desynchronization.
3. **Idle scheduling policy (`crates/tickwire-transport/src/warmup.rs:118-187`)**:
   - Enforces `WarmupInterval > 0`, `in_flight_orders = 0`, and `now_ns - max(last_live_activity_ns, last_warm_ns) >= WarmupInterval`.

## Configurations and invariants
- **`L1_IdleWarmerIsolation.cfg`**: `WarmupInterval = 2`, `MaxNow = 5`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2`.
- **`L1_IdleWarmerIsolation_Disabled.cfg`**: `WarmupInterval = 0`, `MaxNow = 5`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2` (verifies `warm_runs == 0`).
- **Safety invariants**: `TypeOK`, `LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsIdlePolicy`.
