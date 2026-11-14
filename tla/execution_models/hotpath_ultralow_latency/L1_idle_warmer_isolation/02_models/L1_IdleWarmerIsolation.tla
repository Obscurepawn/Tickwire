----------------------- MODULE L1_IdleWarmerIsolation -----------------------
EXTENDS Naturals, Integers

CONSTANTS
    WarmupInterval,
    MaxNow,
    InitialCash,
    InitialQuota,
    MaxLiveOrders,
    MaxWarmRuns

(*
============================ var <-> code map ============================
Code-faithful model of Optimization #2: side-effect-free hot-path execution
cache warming (`maybe_warm_hot_path`) and isolated in-memory loopback `rustls`
warming (`TlsLoopbackWarmer`).

CONSTANTS
  WarmupInterval - `WarmupConfig.warmup_interval_ns` (0 = disabled, >0 = idle threshold):
                   crates/tickwire-transport/src/warmup.rs:56-91
  MaxNow         - finite monotonic clock bound
  InitialCash    - initial free account cash
  InitialQuota   - initial `VenueRateLimiter` / `Quota` token balance
  MaxLiveOrders  - maximum live orders admitted in bounded trace
  MaxWarmRuns    - maximum `WarmHotPath` executions in bounded trace

VARIABLES
  now                          - monotonic clock (`MonoTime`):
                                 crates/tickwire-runtime/src/node.rs:431-453
  last_live_activity           - `WarmupTracker.last_activity_ns` updated on live ticks/orders/acks:
                                 crates/tickwire-transport/src/warmup.rs:93-187
  last_warm_ns                 - `WarmupTracker.last_warm_ns` (-1 before first warmup):
                                 crates/tickwire-transport/src/warmup.rs:93-187
  warm_count                   - completed `WarmHotPath` iterations
  warm_skipped                 - bounded count of policy-suppressed `maybe_warm_hot_path` checks:
                                 crates/tickwire-spot-execution/src/session.rs:1901-1958,
                                 crates/tickwire-spot-runner/src/lib.rs:1216-1231
  live_account_state           - live `Account` cash, open orders, and `StpBook` best ask:
                                 crates/tickwire-spot-runner/src/lib.rs:96-164,
                                 crates/tickwire-spot-runner/src/live.rs:427-429
  settled_spent                - cumulative settled order cash expenditure
  live_journal_seq             - durable `AccountJournal` sequence watermark
  live_intent_seq              - live `Origins` / strategy `intent_seq` watermark
  live_shm_seq                 - live SHM `published` market sequence
  live_quota_tokens            - live `VenueRateLimiter` remaining order quota tokens
  live_inflight                - whether a live order is currently awaiting venue ack:
                                 crates/tickwire-spot-execution/src/session.rs:1915-1934,
                                 crates/tickwire-spot-runner/src/lib.rs:1179-1231
  live_tls_tx_nonce            - live `rustls::ClientConnection` outbound AEAD record sequence:
                                 crates/tickwire-transport/src/connection.rs:253-282
  live_tls_peer_expected_nonce - venue TLS peer expected inbound AEAD record sequence
  live_tls_healthy             - TRUE iff live TLS session has zero `bad_record_mac` errors
  pre_warm_snapshot            - ghost snapshot of all live business & TLS state pre-warmup
  last_step_was_warm           - TRUE iff the most recent transition was `WarmHotPath`
  scratch_account_cash         - isolated scratch `Account` clone balance mutated in dry-run:
                                 crates/tickwire-spot-execution/src/warmup.rs:209-486,
                                 crates/tickwire-binance-execution/src/warmup.rs:190-427
  scratch_sig_digest           - preallocated scratch HMAC-SHA256 request buffer tag:
                                 crates/tickwire-spot-execution/src/warmup.rs:285-398,
                                 crates/tickwire-binance-execution/src/warmup.rs:265-392
  dummy_c2s_nonce              - `TlsLoopbackWarmer` `dummy_client` outbound AEAD nonce:
                                 crates/tickwire-transport/src/warmup.rs:218-419
  dummy_server_expected_nonce  - `TlsLoopbackWarmer` `dummy_server` expected inbound AEAD nonce
  dummy_s2c_nonce              - `TlsLoopbackWarmer` `dummy_server` outbound AEAD ack nonce
  dummy_client_expected_nonce  - `TlsLoopbackWarmer` `dummy_client` expected inbound AEAD nonce
  contrast_stage               - isolated contrast witness showing discarded live TLS ciphertext
                                 desynchronizes AEAD nonces and causes `bad_record_mac`
  contrast_tx_nonce            - contrast shadow client AEAD tx nonce
  contrast_peer_nonce          - contrast shadow server expected AEAD rx nonce
  contrast_bad_record_mac      - contrast shadow `bad_record_mac` latch

FAULTS / INTERLEAVINGS MODELED:
  - arbitrary idle gaps vs. active bursts of market ticks, order sends, acks, and pings;
  - `WarmupInterval = 0` (disabled) vs. `WarmupInterval > 0` (idle-gated);
  - back-to-back `0..MaxWarmRuns` warmups immediately followed by a real live TLS order;
  - negative contrast guard demonstrating why touching a live TLS `writer()` without
    transmitting ciphertext causes irreversible AEAD nonce desynchronization.
==========================================================================
*)

ASSUME
    /\ WarmupInterval >= 0
    /\ MaxNow >= 2
    /\ InitialCash >= MaxLiveOrders
    /\ InitialQuota >= MaxLiveOrders
    /\ MaxLiveOrders >= 1
    /\ MaxWarmRuns >= 1

MaxTlsNonce == MaxLiveOrders + 2

VARIABLES
    now,
    last_live_activity,
    last_warm_ns,
    warm_count,
    warm_skipped,
    live_account_state,
    settled_spent,
    live_journal_seq,
    live_intent_seq,
    live_shm_seq,
    live_quota_tokens,
    live_inflight,
    live_tls_tx_nonce,
    live_tls_peer_expected_nonce,
    live_tls_healthy,
    pre_warm_snapshot,
    last_step_was_warm,
    scratch_account_cash,
    scratch_sig_digest,
    dummy_c2s_nonce,
    dummy_server_expected_nonce,
    dummy_s2c_nonce,
    dummy_client_expected_nonce,
    contrast_stage,
    contrast_tx_nonce,
    contrast_peer_nonce,
    contrast_bad_record_mac

vars == <<
    now, last_live_activity, last_warm_ns, warm_count, warm_skipped,
    live_account_state, settled_spent, live_journal_seq, live_intent_seq,
    live_shm_seq, live_quota_tokens, live_inflight,
    live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
    pre_warm_snapshot, last_step_was_warm,
    scratch_account_cash, scratch_sig_digest,
    dummy_c2s_nonce, dummy_server_expected_nonce,
    dummy_s2c_nonce, dummy_client_expected_nonce,
    contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
>>

CurrentLiveSnapshot == [
    account      |-> live_account_state,
    journal_seq  |-> live_journal_seq,
    intent_seq   |-> live_intent_seq,
    shm_seq      |-> live_shm_seq,
    quota_tokens |-> live_quota_tokens,
    tls_tx_nonce |-> live_tls_tx_nonce,
    tls_rx_exp   |-> live_tls_peer_expected_nonce
]

ShouldWarmNow ==
    /\ WarmupInterval > 0
    /\ now - last_live_activity >= WarmupInterval
    /\ (last_warm_ns < 0 \/ now - last_warm_ns >= WarmupInterval)

Init ==
    /\ now = 0
    /\ last_live_activity = 0
    /\ last_warm_ns = -1
    /\ warm_count = 0
    /\ warm_skipped = 0
    /\ live_account_state = [
           cash_free    |-> InitialCash,
           cash_locked  |-> 0,
           open_orders  |-> 0,
           stp_best_ask |-> 5
       ]
    /\ settled_spent = 0
    /\ live_journal_seq = 0
    /\ live_intent_seq = 0
    /\ live_shm_seq = 0
    /\ live_quota_tokens = InitialQuota
    /\ live_inflight = FALSE
    /\ live_tls_tx_nonce = 0
    /\ live_tls_peer_expected_nonce = 0
    /\ live_tls_healthy = TRUE
    /\ pre_warm_snapshot = [
           account      |-> [
               cash_free    |-> InitialCash,
               cash_locked  |-> 0,
               open_orders  |-> 0,
               stp_best_ask |-> 5
           ],
           journal_seq  |-> 0,
           intent_seq   |-> 0,
           shm_seq      |-> 0,
           quota_tokens |-> InitialQuota,
           tls_tx_nonce |-> 0,
           tls_rx_exp   |-> 0
       ]
    /\ last_step_was_warm = FALSE
    /\ scratch_account_cash = InitialCash
    /\ scratch_sig_digest = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_server_expected_nonce = 0
    /\ dummy_s2c_nonce = 0
    /\ dummy_client_expected_nonce = 0
    /\ contrast_stage = 0
    /\ contrast_tx_nonce = 0
    /\ contrast_peer_nonce = 0
    /\ contrast_bad_record_mac = FALSE

\* Monotonic clock tick (`crates/tickwire-runtime/src/node.rs:431-453`).
AdvanceClock ==
    /\ now < MaxNow
    /\ now' = now + 1
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        last_live_activity, last_warm_ns, warm_count, warm_skipped,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_shm_seq, live_quota_tokens, live_inflight,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Live market tick advances SHM sequence and resets idle timer (`crates/tickwire-transport/src/warmup.rs:124-135`).
LiveMarketTick ==
    /\ live_shm_seq < 2
    /\ live_shm_seq' = live_shm_seq + 1
    /\ last_live_activity' = now
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_warm_ns, warm_count, warm_skipped,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_quota_tokens, live_inflight,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Live order admission commits Account + AccountJournal + Quota and transmits real TLS record (`crates/tickwire-transport/src/connection.rs:253-282`).
LiveOrderAdmitAndSend ==
    /\ ~live_inflight
    /\ live_tls_healthy
    /\ live_intent_seq < MaxLiveOrders
    /\ live_quota_tokens > 0
    /\ live_account_state.cash_free > 0
    /\ live_account_state' = [
           live_account_state EXCEPT
               !.cash_free = @ - 1,
               !.cash_locked = @ + 1,
               !.open_orders = @ + 1
       ]
    /\ live_journal_seq' = live_journal_seq + 1
    /\ live_intent_seq' = live_intent_seq + 1
    /\ live_quota_tokens' = live_quota_tokens - 1
    /\ live_inflight' = TRUE
    /\ live_tls_healthy' = (live_tls_tx_nonce = live_tls_peer_expected_nonce)
    /\ live_tls_tx_nonce' = live_tls_tx_nonce + 1
    /\ live_tls_peer_expected_nonce' =
           IF live_tls_tx_nonce = live_tls_peer_expected_nonce
           THEN live_tls_peer_expected_nonce + 1
           ELSE live_tls_peer_expected_nonce
    /\ last_live_activity' = now
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_warm_ns, warm_count, warm_skipped, settled_spent, live_shm_seq,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Live venue acknowledgement settles the open order and resets idle timer (`crates/tickwire-spot-runner/src/live.rs:427-429`).
LiveVenueAck ==
    /\ live_inflight
    /\ live_account_state' = [
           live_account_state EXCEPT
               !.cash_locked = @ - 1,
               !.open_orders = @ - 1
       ]
    /\ settled_spent' = settled_spent + 1
    /\ live_inflight' = FALSE
    /\ last_live_activity' = now
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_warm_ns, warm_count, warm_skipped,
        live_journal_seq, live_intent_seq, live_shm_seq, live_quota_tokens,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Compliant application-layer Ping/Pong keeps live socket warm by actually transmitting TLS frame.
LiveAppPing ==
    /\ live_tls_healthy
    /\ live_tls_tx_nonce < MaxTlsNonce
    /\ live_tls_tx_nonce = live_intent_seq
    /\ live_tls_healthy' = (live_tls_tx_nonce = live_tls_peer_expected_nonce)
    /\ live_tls_tx_nonce' = live_tls_tx_nonce + 1
    /\ live_tls_peer_expected_nonce' = live_tls_peer_expected_nonce + 1
    /\ last_live_activity' = now
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_warm_ns, warm_count, warm_skipped,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_shm_seq, live_quota_tokens, live_inflight, pre_warm_snapshot,
        scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* maybe_warm_hot_path fast-returns NoOp when disabled or inside active window (`crates/tickwire-transport/src/warmup.rs:146-175`, `crates/tickwire-spot-execution/src/session.rs:1901-1958`).
SkipWarmWhenDisabledOrBusy ==
    /\ ~ShouldWarmNow
    /\ warm_skipped < 1
    /\ warm_skipped' = warm_skipped + 1
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_live_activity, last_warm_ns, warm_count,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_shm_seq, live_quota_tokens, live_inflight,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Side-effect-free hot-path dry-run + isolated TlsLoopbackWarmer C2S/S2C roundtrip (`crates/tickwire-transport/src/warmup.rs:344-419`, `crates/tickwire-spot-execution/src/warmup.rs:209-486`, `crates/tickwire-binance-execution/src/warmup.rs:190-427`).
WarmHotPath ==
    /\ ShouldWarmNow
    /\ warm_count < MaxWarmRuns
    /\ pre_warm_snapshot' = CurrentLiveSnapshot
    /\ last_step_was_warm' = TRUE
    /\ last_warm_ns' = now
    /\ warm_count' = warm_count + 1
    \* Scratch Account check + scratch HMAC-SHA256 signing (never touches live_account_state):
    /\ scratch_account_cash' = IF scratch_account_cash > 0 THEN scratch_account_cash - 1 ELSE InitialCash
    /\ scratch_sig_digest' = warm_count + 1
    \* Isolated in-memory loopback rustls ClientConnection <-> ServerConnection roundtrip:
    /\ dummy_c2s_nonce' = dummy_c2s_nonce + 1
    /\ dummy_server_expected_nonce' = dummy_server_expected_nonce + 1
    /\ dummy_s2c_nonce' = dummy_s2c_nonce + 1
    /\ dummy_client_expected_nonce' = dummy_client_expected_nonce + 1
    /\ UNCHANGED <<
        now, last_live_activity, warm_skipped,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_shm_seq, live_quota_tokens, live_inflight,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        contrast_stage, contrast_tx_nonce, contrast_peer_nonce, contrast_bad_record_mac
       >>

\* Negative contrast witness: proves why writing to a live TLS session and discarding ciphertext
\* causes irreversible AEAD sequence desync (`bad_record_mac`), and why WarmHotPath forbids it.
DemonstrateForbiddenLiveTlsDiscardContrast ==
    /\ contrast_stage < 2
    /\ IF contrast_stage = 0 THEN
           /\ contrast_stage' = 1
           /\ contrast_tx_nonce' = contrast_tx_nonce + 1
           /\ UNCHANGED <<contrast_peer_nonce, contrast_bad_record_mac>>
       ELSE
           /\ contrast_stage' = 2
           /\ contrast_bad_record_mac' = (contrast_tx_nonce # contrast_peer_nonce)
           /\ contrast_tx_nonce' = contrast_tx_nonce + 1
           /\ UNCHANGED contrast_peer_nonce
    /\ last_step_was_warm' = FALSE
    /\ UNCHANGED <<
        now, last_live_activity, last_warm_ns, warm_count, warm_skipped,
        live_account_state, settled_spent, live_journal_seq, live_intent_seq,
        live_shm_seq, live_quota_tokens, live_inflight,
        live_tls_tx_nonce, live_tls_peer_expected_nonce, live_tls_healthy,
        pre_warm_snapshot, scratch_account_cash, scratch_sig_digest,
        dummy_c2s_nonce, dummy_server_expected_nonce,
        dummy_s2c_nonce, dummy_client_expected_nonce
       >>

Next ==
    \/ AdvanceClock
    \/ LiveMarketTick
    \/ LiveOrderAdmitAndSend
    \/ LiveVenueAck
    \/ LiveAppPing
    \/ SkipWarmWhenDisabledOrBusy
    \/ WarmHotPath
    \/ DemonstrateForbiddenLiveTlsDiscardContrast

Spec == Init /\ [][Next]_vars

\* =============================== Invariants ===============================

TypeOK ==
    /\ now \in 0..MaxNow
    /\ last_live_activity \in 0..MaxNow
    /\ last_warm_ns \in (-1)..MaxNow
    /\ warm_count \in 0..MaxWarmRuns
    /\ warm_skipped \in 0..1
    /\ live_account_state.cash_free \in 0..InitialCash
    /\ live_account_state.cash_locked \in 0..InitialCash
    /\ live_account_state.open_orders \in 0..MaxLiveOrders
    /\ live_account_state.stp_best_ask = 5
    /\ settled_spent \in 0..InitialCash
    /\ live_journal_seq \in 0..MaxLiveOrders
    /\ live_intent_seq \in 0..MaxLiveOrders
    /\ live_shm_seq \in 0..2
    /\ live_quota_tokens \in 0..InitialQuota
    /\ live_inflight \in BOOLEAN
    /\ live_tls_tx_nonce \in 0..MaxTlsNonce
    /\ live_tls_peer_expected_nonce \in 0..MaxTlsNonce
    /\ live_tls_healthy \in BOOLEAN
    /\ last_step_was_warm \in BOOLEAN
    /\ scratch_account_cash \in 0..InitialCash
    /\ scratch_sig_digest \in 0..MaxWarmRuns
    /\ dummy_c2s_nonce \in 0..MaxWarmRuns
    /\ dummy_server_expected_nonce \in 0..MaxWarmRuns
    /\ dummy_s2c_nonce \in 0..MaxWarmRuns
    /\ dummy_client_expected_nonce \in 0..MaxWarmRuns
    /\ contrast_stage \in 0..2
    /\ contrast_tx_nonce \in 0..2
    /\ contrast_peer_nonce = 0
    /\ contrast_bad_record_mac \in BOOLEAN

\* WarmHotPath NEVER alters live Account, AccountJournal, intent_seq, SHM seq, or Quota tokens.
LiveBusinessStateInvariance ==
    /\ (last_step_was_warm => CurrentLiveSnapshot = pre_warm_snapshot)
    /\ live_account_state.cash_free + live_account_state.cash_locked + settled_spent = InitialCash
    /\ live_journal_seq = live_intent_seq
    /\ InitialQuota - live_quota_tokens = live_intent_seq

\* Live TLS AEAD nonces remain strictly synchronized across 0..N WarmHotPath runs + live orders.
LiveTlsNonceSynchronization ==
    /\ live_tls_tx_nonce = live_tls_peer_expected_nonce
    /\ live_tls_healthy = TRUE
    /\ (contrast_stage = 2 => (contrast_bad_record_mac /\ contrast_tx_nonce # contrast_peer_nonce))

\* Isolated TlsLoopbackWarmer client/server AEAD nonces advance strictly in lockstep.
DummyLoopbackTlsPairSynchronization ==
    /\ dummy_c2s_nonce = dummy_server_expected_nonce
    /\ dummy_s2c_nonce = dummy_client_expected_nonce
    /\ dummy_c2s_nonce = warm_count
    /\ dummy_s2c_nonce = warm_count

\* Warmup never runs when disabled (WarmupInterval = 0) or inside active window (< WarmupInterval).
WarmupStrictlyRespectsIdlePolicy ==
    /\ (WarmupInterval = 0 => (warm_count = 0 /\ last_warm_ns = -1))
    /\ (last_step_was_warm =>
        /\ WarmupInterval > 0
        /\ now - last_live_activity >= WarmupInterval
        /\ last_warm_ns = now)

=============================================================================
