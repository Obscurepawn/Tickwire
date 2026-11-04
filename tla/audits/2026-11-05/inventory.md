# Source-anchor inventory

Source revision `9b2ed6307219`. Scope includes all 55 Cargo workspace crates.
A family name records a production source reference in a level input, model or line lock. A reference can supply a caller assumption. It does not establish coverage of every function in that crate. A dash means the audited artifacts contain no explicit production source path for the crate.

| Crate | Previous families | Families added in this audit |
| --- | --- | --- |
| `tickwire-account` | account, account_gateway, account_journal | binance_roundtrip, cash_evidence, risk_limits, spot_roundtrip, strategy_intake |
| `tickwire-account-codec` | — | — |
| `tickwire-account-gateway` | account_gateway | binance_roundtrip, cash_evidence, spot_roundtrip, strategy_intake |
| `tickwire-account-journal` | account_gateway, account_journal | cash_evidence |
| `tickwire-app` | — | binance_roundtrip, quote_delivery, spot_roundtrip, strategy_intake, worker_handover |
| `tickwire-binance` | binance_depth_sync | — |
| `tickwire-binance-account` | — | binance_roundtrip |
| `tickwire-binance-clock` | system_clock | — |
| `tickwire-binance-execution` | — | binance_roundtrip |
| `tickwire-binance-private` | binance_private_session | — |
| `tickwire-binance-public` | — | — |
| `tickwire-binance-quota` | — | request_budget |
| `tickwire-binance-sbe` | — | — |
| `tickwire-binance-session` | — | — |
| `tickwire-book` | binance_depth_sync, book_state | — |
| `tickwire-business-journal` | business_journal, gateway | — |
| `tickwire-clock` | clock, system_clock | — |
| `tickwire-clock-sys` | system_clock | — |
| `tickwire-config` | — | — |
| `tickwire-config-fs` | — | — |
| `tickwire-config-journal` | — | config_activation |
| `tickwire-config-materializer` | — | config_activation |
| `tickwire-control` | control_requests | — |
| `tickwire-control-store` | — | — |
| `tickwire-control-sys` | — | — |
| `tickwire-execution` | business_journal, mock_abort, submission | — |
| `tickwire-gateway` | gateway | — |
| `tickwire-instrument-catalog` | — | — |
| `tickwire-instrument-index` | — | — |
| `tickwire-ipc` | — | — |
| `tickwire-join` | join_owner | — |
| `tickwire-journal` | account_journal, physical_journal | — |
| `tickwire-journal-codec` | business_journal | — |
| `tickwire-lab` | — | binance_roundtrip |
| `tickwire-market-publisher` | — | quote_delivery, spot_roundtrip |
| `tickwire-market-source` | — | binance_roundtrip, quote_delivery, spot_roundtrip, worker_handover |
| `tickwire-md` | — | quote_delivery |
| `tickwire-mock` | mock_abort, submission | — |
| `tickwire-quota` | — | request_budget |
| `tickwire-recovery` | business_journal, recovery_closure | — |
| `tickwire-release` | — | — |
| `tickwire-risk` | — | risk_limits |
| `tickwire-runtime` | — | binance_roundtrip |
| `tickwire-runtime-policy` | — | binance_roundtrip, quote_delivery, spot_roundtrip |
| `tickwire-shm` | broadcast_ring, join_owner | quote_delivery |
| `tickwire-shm-sys` | — | — |
| `tickwire-snapshot` | — | — |
| `tickwire-spot-execution` | — | spot_roundtrip, strategy_intake |
| `tickwire-spot-runner` | spot_account_recovery | cash_evidence, risk_limits, spot_roundtrip, strategy_intake |
| `tickwire-strategy-example` | — | — |
| `tickwire-strategy-host` | — | binance_roundtrip, worker_handover |
| `tickwire-strategy-sdk` | — | binance_roundtrip, quote_delivery, worker_handover |
| `tickwire-supervisor` | — | worker_handover |
| `tickwire-transport` | account_gateway, binance_private_session, gateway, transport | binance_roundtrip, spot_roundtrip |
| `tickwire-venue` | — | — |

21 crates had previous references. 16 have no direct source reference after this audit.

## Previous line locks

For each pre-existing level, the script compares the stored SHA-256 at each recorded file and line against the pinned Rust tree. A moved line counts as a mismatch. Contiguous recorded ranges are classified separately. A verbatim match at one other position in the same file counts as relocated. A changed, missing or ambiguous range needs a source review. A matching lock checks the cited text only. The historical input assumptions can change elsewhere.

| Level | Recorded lines | Changed lines | Relocated ranges | Ranges needing review |
| --- | ---: | ---: | ---: | ---: |
| `account/L0_spot_facts` | 672 | 532 | 1 | 9 |
| `account/L1_reduce_shadow` | 867 | 781 | 5 | 8 |
| `account_gateway/L0_guarded_account_output` | 1576 | 1138 | 1 | 16 |
| `account_journal/L0_ordered_recovery` | 582 | 389 | 2 | 12 |
| `binance_depth_sync/L0_coordinator` | 575 | 356 | 0 | 2 |
| `binance_private_session/L0_session_lifecycle` | 1116 | 0 | 0 | 0 |
| `book_state/L0_lifecycle` | 214 | 0 | 0 | 0 |
| `book_state/L1_coverage` | 211 | 0 | 0 | 0 |
| `broadcast_ring/L0_publish_borrow_reclaim` | 720 | 716 | 9 | 3 |
| `business_journal/L0_commit_abort_recovery` | 337 | 270 | 1 | 3 |
| `business_journal/L1_command_binding` | 268 | 216 | 2 | 4 |
| `clock/L0_calibration` | 316 | 306 | 6 | 0 |
| `control_requests/L0_registry` | 138 | 0 | 0 | 0 |
| `control_requests/L1_session` | 246 | 0 | 0 | 0 |
| `gateway/L0_journaled_sender` | 670 | 62 | 3 | 1 |
| `join_owner/L0_pending_activation` | 1221 | 902 | 1 | 2 |
| `mock_abort/L0_ordered_abort` | 426 | 347 | 7 | 3 |
| `physical_journal/L0_batch_sync_recovery` | 447 | 370 | 2 | 4 |
| `recovery_closure/L0_publish_verify` | 214 | 212 | 2 | 2 |
| `spot_account_recovery/L0_private_cut` | No lock | — | 0 | 0 |
| `submission/L0_commit_gate` | 135 | 0 | 0 | 0 |
| `submission/L1_crash_receipt` | 269 | 0 | 0 | 0 |
| `submission/L2_economic_facts` | 444 | 430 | 4 | 4 |
| `system_clock/L0_native_owner` | 231 | 34 | 3 | 0 |
| `transport/L0_first_output` | 231 | 0 | 0 | 0 |

24 of 25 previous levels have locks. 16 locks contain a mismatch. 7,061 of 12,126 recorded line references differ.

Reproduce with `python3 tla/audits/2026-11-05/inventory.py` at the audited checkout.
