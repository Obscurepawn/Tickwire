# 实际 TLC 检查

三个配置均以单 worker 完整穷举，最终队列为零。执行器使用仓库 `tla/config.env` 中的 TLC 2.19 工具。

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal.tla tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal.cfg --det --coverage
```

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal.tla tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal_two.cfg --det --coverage
```

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal.tla tla/execution_models/account_journal/L0_ordered_recovery/02_models/L0_AccountJournal_crashes.cfg --det --coverage
```

| 配置 | Orders | Capital | MaxRecords | CrashLimit | generated | distinct | 深度 | 时间 |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| L0_AccountJournal | 1 | 1 | 3 | 1 | 220837 | 52700 | 52 | 05s |
| L0_AccountJournal_two | 2 | 1 | 3 | 1 | 3211231 | 748060 | 52 | 01min 19s |
| L0_AccountJournal_crashes | 1 | 1 | 3 | 2 | 615434 | 125574 | 66 | 13s |

下表取最后一个 coverage 块的 generated 列。每个配置的 Init 和 42 个 Next 动作均大于零；distinct=0 的精确重试仍有实际覆盖。

| 动作 | primary | two | crashes |
|---|---:|---:|---:|
| Init | 7 | 7 | 7 |
| StartCreate | 18 | 18 | 27 |
| RejectCreate | 10486 | 149956 | 31315 |
| CreateFile | 54 | 54 | 81 |
| WriteFile | 54 | 54 | 81 |
| SyncFile | 54 | 54 | 81 |
| SyncDirectory | 54 | 54 | 81 |
| CreateFailure | 216 | 216 | 324 |
| PrepareAdmit | 756 | 16200 | 1134 |
| PrepareAbort | 1800 | 21576 | 2936 |
| RetryAdmit | 1836 | 29160 | 2754 |
| ConflictAdmit | 1836 | 29160 | 2754 |
| RejectPoison | 9684 | 144624 | 15234 |
| RejectRecoveredAdmit | 518 | 7392 | 1049 |
| RejectCapacity | 1724 | 32688 | 2740 |
| WriteTail | 2556 | 37776 | 4070 |
| SealBatch | 2556 | 37776 | 4070 |
| SyncBatch | 2556 | 37776 | 4070 |
| PublishReceipt | 2556 | 37776 | 4070 |
| AppendFailure | 10224 | 151104 | 16280 |
| StartRecover | 122880 | 1795024 | 368400 |
| CheckConfig | 2372 | 33588 | 6954 |
| RejectConfig | 2484 | 33700 | 7130 |
| CheckPhysical | 2320 | 33536 | 6874 |
| RejectPhysical | 52 | 52 | 80 |
| CheckRequired | 2320 | 33536 | 6874 |
| RejectRequired | 796 | 11216 | 2358 |
| SyncRecovery | 1524 | 22320 | 4516 |
| RecoveryIoFailure | 11072 | 156732 | 32428 |
| CheckStructure | 1442 | 21932 | 3988 |
| RejectStructure | 82 | 388 | 528 |
| VerifyClosure | 308 | 7144 | 712 |
| RejectClosure | 616 | 14288 | 1232 |
| VerifierUnavailable | 924 | 21432 | 1944 |
| SkipClosure | 1902 | 19708 | 6240 |
| AdvanceHistory | 2210 | 26852 | 6952 |
| BeginReplay | 826 | 7644 | 2756 |
| ReplayAdmit | 66 | 1172 | 228 |
| ReplayAbort | 36 | 232 | 159 |
| RejectReplay | 764 | 7320 | 2549 |
| FreezeRecovered | 62 | 324 | 207 |
| ReturnRecovered | 62 | 324 | 207 |
| Crash | 16172 | 229346 | 58960 |

16 项不变量见配置；源码、模型、配置、工具、锚点与最终日志 SHA-256 见 `04_evidence/results.json`。日志已去除机器路径及行尾空白，TLC 统计未改写。锚点脚本独立扫描本层 582 行，校验为零漂移。

早期可运行模型遗漏目录重同步，详情见 `04_counterexamples.md`。此外已修正 TLA 排版、字符串与序列比较，并按源码把物理 writer 检查移到截尾之前；这些调整没有改动 Rust 源码。
