# Modeling reasoning — `L1_IdleWarmerIsolation`

## Scope and state owners
1. **Live single-writer trading and TLS pipeline (`crates/tickwire-spot-runner/src/lib.rs:96-164`, `crates/tickwire-spot-execution/src/session.rs:1815-1958`, `crates/tickwire-binance-execution/src/lib.rs:695-835`, `crates/tickwire-transport/src/connection.rs:253-282`)**:
   - Live actions (`LiveMarketTick`, `LiveOrderAdmitAndSend`, `LiveVenueAck`, `LiveAppPing`, `AdvanceClock`) exclusively own `live_account_state` (`cash_free`, `cash_locked`, `open_orders`, `stp_best_ask`), `live_journal_seq`, `live_intent_seq`, `live_shm_seq`, `live_quota_tokens`, and the live TLS session's AEAD outbound/peer sequence numbers (`live_tls_tx_nonce`, `live_tls_peer_expected_nonce`).
   - Every live TLS plaintext write (`LiveOrderAdmitAndSend`, `LiveAppPing`) flushes its encrypted record to the peer, advancing `live_tls_tx_nonce` and `live_tls_peer_expected_nonce` together.
2. **Side-effect-free hot-path dry-run & isolated `TlsLoopbackWarmer` (`crates/tickwire-transport/src/warmup.rs:56-419`, `crates/tickwire-spot-execution/src/warmup.rs:209-486`, `crates/tickwire-binance-execution/src/warmup.rs:190-427`, `crates/tickwire-runtime/src/node.rs:431-453`)**:
   - `WarmupTracker` gates `WarmHotPath` on `WarmupInterval > 0`, `now - last_live_activity >= WarmupInterval`, and `last_warm_ns < 0 \/ now - last_warm_ns >= WarmupInterval` (with session/runner guards also suppressing warmup while any order/reply is in flight); otherwise `SkipWarmWhenDisabledOrBusy` leaves both live and warmer state untouched.
   - `WarmHotPath` exercises stack reply/quote decoding, read-only `stp_book` + cloned `scratch_account` dry-run (`scratch_account_cash`), preallocated scratch HMAC-SHA256 signing (`scratch_sig_digest`), and an isolated in-memory TLS 1.3 loopback pair (`dummy_client <-> dummy_server` with `send_tls13_tickets = 0`, advancing `dummy_c2s_nonce`/`dummy_server_expected_nonce` and `dummy_s2c_nonce`/`dummy_client_expected_nonce` in lockstep).
   - A ghost snapshot (`pre_warm_snapshot`, `last_step_was_warm`) captures the entire live state tuple before every `WarmHotPath` execution so `LiveBusinessStateInvariance` verifies bit-for-bit equality across every reachable state.
3. **Negative/contrast guard (`DemonstrateForbiddenLiveTlsDiscardContrast`)**:
   - Explicitly models why writing dummy plaintext into a live `rustls::ClientConnection` and discarding the resulting ciphertext is forbidden: advancing `contrast_tx_nonce` without delivering ciphertext to `contrast_peer_nonce` deterministically triggers `contrast_bad_record_mac = TRUE` on the next send, whereas routing all warmup writes exclusively through `TlsLoopbackWarmer` keeps `live_tls_tx_nonce = live_tls_peer_expected_nonce` across arbitrary `0..N` warmups interleaved with real orders.

## Configurations and invariants
- **`L1_IdleWarmerIsolation.cfg`**: `WarmupInterval = 2`, `MaxNow = 5`, `InitialCash = 3`, `InitialQuota = 2`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2`.
- **`L1_IdleWarmerIsolation_Disabled.cfg`**: `WarmupInterval = 0`, `MaxNow = 5`, `InitialCash = 3`, `InitialQuota = 2`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2`.
- **Invariants**: `TypeOK`, `LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsIdlePolicy`.
