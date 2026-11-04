# Previously submitted TLC configurations

The original revision contains 19 model families and 25 levels. Each row refers to the exact model and constants tracked at `9b2ed63`. The run uses the pinned TLC 2.19 jar. A pass requires a complete breadth-first search, an empty queue and a readable final action-coverage block.

**Run results** across 85 configurations — 80 pass, 5 incomplete. Counts are per configuration. The same state may occur in several configurations.

| Family | Configuration | Result | Distinct states | Depth | Actions exercised / reported, or last queue |
| --- | --- | --- | ---: | ---: | --- |
| account | `L0_SpotFacts_v3` | Pass | 176596 | 16 | 22/22 |
| account | `L0_SpotFacts_v3_tight` | Pass | 180131 | 16 | 22/22 |
| account | `L0_SpotFacts_v3_two` | Pass | 192984 | 15 | 22/22 |
| account | `L1_ReduceShadow` | Pass | 136551 | 16 | 23/23 |
| account | `L1_ReduceShadow_open` | Pass | 24510 | 15 | 23/23 |
| account | `L1_ReduceShadow_tight` | Pass | 99367 | 16 | 23/23 |
| account_gateway | `L0_AccountGateway_v2` | Pass | 961560 | 50 | 57/58 |
| account_gateway | `L0_AccountGateway_v2_boundary` | Pass | 892832 | 50 | 57/58 |
| account_gateway | `L0_AccountGateway_v2_crashes` | Pass | 1793656 | 50 | 57/58 |
| account_gateway | `L0_AccountGateway_v2_two` | Pass | 2024848 | 50 | 57/58 |
| account_journal | `L0_AccountJournal` | Pass | 52700 | 52 | 43/43 |
| account_journal | `L0_AccountJournal_crashes` | Pass | 125574 | 66 | 43/43 |
| account_journal | `L0_AccountJournal_two` | Pass | 748060 | 52 | 43/43 |
| binance_depth_sync | `L0_DepthSync` | Pass | 73269 | 14 | 23/23 |
| binance_depth_sync | `L0_SpotBuffered` | Incomplete | 12,231,368 | 10 | 4,397,674 queued |
| binance_depth_sync | `L0_UsdM` | Pass | 73897 | 13 | 23/23 |
| binance_depth_sync | `L0_UsdMBuffered` | Incomplete | 18,303,785 | 11 | 6,185,629 queued |
| binance_private_session | `L0_PrivateSession_v2` | Pass | 12018 | 27 | 32/32 |
| binance_private_session | `L0_PrivateSession_v2_buffer` | Pass | 1043 | 20 | 32/32 |
| binance_private_session | `L0_PrivateSession_v2_logon` | Pass | 1881 | 28 | 32/32 |
| binance_private_session | `L0_PrivateSession_v2_usdm` | Pass | 267643 | 34 | 30/32 |
| book_state | `L0_BookState` | Pass | 1392 | 6 | 20/20 |
| book_state | `L0_SpotScaling` | Pass | 8499 | 6 | 20/20 |
| book_state | `L0_UsdM` | Pass | 1880 | 6 | 20/20 |
| book_state | `L0_UsdMScaling` | Pass | 10524 | 6 | 20/20 |
| book_state | `L1_Ask` | Pass | 1471 | 6 | 16/16 |
| book_state | `L1_AskScaling` | Pass | 8244 | 6 | 16/16 |
| book_state | `L1_BidScaling` | Pass | 7050 | 6 | 16/16 |
| book_state | `L1_BookCoverage` | Pass | 1471 | 6 | 16/16 |
| broadcast_ring | `L0_BroadcastRing` | Pass | 29889 | 25 | 18/18 |
| broadcast_ring | `L0_BroadcastRing_pending_wrap` | Pass | 12834 | 21 | 17/17 |
| broadcast_ring | `L0_BroadcastRing_two_readers` | Pass | 930364 | 33 | 18/18 |
| broadcast_ring | `L0_BroadcastRing_wrap` | Pass | 11324 | 21 | 16/16 |
| broadcast_ring | `L0_BroadcastRing_wrap_two` | Pass | 40796 | 24 | 16/16 |
| business_journal | `L0_BusinessJournal` | Pass | 72 | 14 | 24/24 |
| business_journal | `L0_BusinessJournal_competing` | Pass | 345 | 26 | 24/24 |
| business_journal | `L0_BusinessJournal_scaling` | Pass | 879 | 28 | 24/24 |
| business_journal | `L1_CommandBinding` | Pass | 376 | 15 | 30/30 |
| business_journal | `L1_CommandBinding_competing` | Pass | 2664 | 27 | 30/30 |
| business_journal | `L1_CommandBinding_recovery` | Pass | 7326 | 29 | 30/30 |
| clock | `L0_Calibration` | Incomplete | 538,154 | 11 | 300,040 queued |
| clock | `L0_Calibration_boundary` | Pass | 64292 | 12 | 16/16 |
| clock | `L0_Calibration_rounding` | Incomplete | 386,289 | 8 | 250,041 queued |
| clock | `L0_Calibration_usdm` | Incomplete | 813,196 | 9 | 495,634 queued |
| control_requests | `L0_ControlRequests` | Pass | 370 | 6 | 5/5 |
| control_requests | `L0_ControlRequests_pending` | Pass | 13442 | 10 | 5/5 |
| control_requests | `L0_ControlRequests_two` | Pass | 1570 | 8 | 5/5 |
| control_requests | `L1_ControlSession` | Pass | 1566 | 8 | 8/8 |
| control_requests | `L1_ControlSession_renew` | Pass | 3804 | 9 | 8/8 |
| control_requests | `L1_ControlSession_two` | Pass | 3926 | 8 | 8/8 |
| gateway | `larger_prefix` | Pass | 11188 | 42 | 42/42 |
| gateway | `primary` | Pass | 7572 | 42 | 42/42 |
| gateway | `three_intents` | Pass | 57624 | 60 | 42/42 |
| gateway | `two_crashes` | Pass | 8610 | 42 | 42/42 |
| join_owner | `L0_JoinOwner` | Pass | 2181 | 23 | 23/23 |
| join_owner | `L0_JoinOwner_two` | Pass | 321256 | 35 | 23/23 |
| join_owner | `L0_JoinOwner_wrap` | Pass | 172 | 14 | 17/17 |
| join_owner | `L0_JoinOwner_wrap_two` | Pass | 7652 | 25 | 17/17 |
| mock_abort | `L0_OrderedAbort` | Pass | 2051 | 26 | 16/16 |
| mock_abort | `L0_OrderedAbort_boundary` | Pass | 457 | 22 | 16/16 |
| mock_abort | `L0_OrderedAbort_scaling` | Pass | 102149 | 40 | 16/16 |
| physical_journal | `L0_PhysicalJournal` | Pass | 1816 | 18 | 17/17 |
| physical_journal | `L0_PhysicalJournal_batch` | Pass | 2695 | 20 | 17/17 |
| physical_journal | `L0_PhysicalJournal_scaling` | Pass | 45883 | 28 | 17/17 |
| recovery_closure | `L0_Conflict` | Pass | 15097 | 56 | 23/23 |
| recovery_closure | `L0_MultipleDependencies` | Pass | 3898 | 66 | 23/23 |
| recovery_closure | `L0_RecoveryClosure` | Pass | 2345 | 51 | 23/23 |
| spot_account_recovery | `L0_PrivateCut` | Pass | 1126 | 17 | 11/11 |
| submission | `L0_CommitGate` | Pass | 1044 | 13 | 12/12 |
| submission | `L0_CommitGate_scaling` | Pass | 4108 | 13 | 12/12 |
| submission | `L1_CrashReceipt` | Pass | 784 | 13 | 20/20 |
| submission | `L1_CrashReceipt_pending` | Pass | 53544 | 22 | 20/20 |
| submission | `L1_CrashReceipt_scaling` | Pass | 28096 | 22 | 20/20 |
| submission | `L2_EconomicFacts` | Pass | 24036 | 8 | 19/19 |
| submission | `L2_EconomicFacts_boundary` | Pass | 1495032 | 11 | 19/19 |
| submission | `L2_EconomicFacts_long` | Pass | 280458 | 9 | 19/19 |
| submission | `L2_EconomicFacts_scaling` | Pass | 1485288 | 11 | 19/19 |
| system_clock | `L0_NativeOwner` | Pass | 3021 | 7 | 15/15 |
| system_clock | `L0_NativeOwner_generic` | Pass | 2431 | 7 | 7/7 |
| system_clock | `L0_NativeOwner_long` | Pass | 18714 | 9 | 15/15 |
| system_clock | `L0_NativeOwner_unsupported` | Pass | 2 | 2 | 2/2 |
| transport | `larger_buffer` | Pass | 5944 | 20 | 20/20 |
| transport | `plain` | Pass | 2832 | 13 | 17/17 |
| transport | `primary` | Pass | 688 | 14 | 20/20 |
| transport | `two_sends` | Pass | 3928 | 20 | 20/20 |

The five incomplete rows reached the audit’s 1,800-second wall-clock limit with a nonempty queue. The table records the last progress line before termination. Depth in those rows is only the depth reached. The buffered Binance futures run used two workers. [Last progress reports](legacy-time-limits.txt) · [Original clock results](../../execution_models/clock/L0_calibration/03_run_log.md) · [Original Binance depth results](../../execution_models/binance_depth_sync/L0_coordinator/03_run_log.md)

Reproduce a family with `./scripts/check-tla.sh FAMILY`. To select one configuration, copy the family’s `02_models` source and config into a scratch directory and run `java -XX:+UseParallelGC -cp /absolute/path/to/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -config CONFIG.cfg MODEL.tla`. The raw run count in this table and the [source-anchor inventory](inventory.md) answer different questions.
