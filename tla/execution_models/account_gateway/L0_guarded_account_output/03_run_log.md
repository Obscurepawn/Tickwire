# 实际 TLC 检查

四个配置以单 worker 完整穷举，最后队列均为零。实际运行使用隔离目录中的相同模型与配置；完成后按原字节复制到本目录，摘要逐项核对。下列命令从仓库路径复现相同输入。日志仅统一机器路径、行尾空白和文件末尾换行。

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2.tla tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2.cfg --det --coverage
```

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2.tla tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2_boundary.cfg --det --coverage
```

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2.tla tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2_two.cfg --det --coverage
```

```sh
bash "$HOME/.codex/skills/vibe-tla/scripts/run_tlc.sh" tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2.tla tla/execution_models/account_gateway/L0_guarded_account_output/02_models/L0_AccountGateway_v2_crashes.cfg --det --coverage
```

| 配置 | generated | distinct | 深度 | 时间 |
|---|---:|---:|---:|---|
| L0_AccountGateway_v2 | 5967655 | 961560 | 50 | 02min 35s |
| L0_AccountGateway_v2_boundary | 5456397 | 892832 | 50 | 02min 03s |
| L0_AccountGateway_v2_two | 12639561 | 2024848 | 50 | 05min 06s |
| L0_AccountGateway_v2_crashes | 9600215 | 1793656 | 50 | 04min 13s |

下表取最终 coverage 块的 generated 列。AdmissionOverflow 由上限配置覆盖；常规配置保留足够序号空间。上限配置的 QueueFail 为零：第一次准入与中止已耗尽其可增加次数，下一意图在准入时溢出，无法到达连接关闭后的入队失败；其他配置覆盖该分支。所有 Next 动作在组合配置中都有生成后继的见证。

| 动作 | primary | boundary | two | crashes |
|---|---:|---:|---:|---:|
| Init | 1 | 1 | 1 | 1 |
| RejectPreparation | 3264 | 3600 | 6464 | 3264 |
| Prepare | 216 | 48 | 168 | 216 |
| RejectAdmission | 1760 | 1928 | 9168 | 1760 |
| AdmissionOverflow | 0 | 288 | 0 | 0 |
| ClosureOk | 464 | 128 | 672 | 464 |
| ClosureFail | 464 | 128 | 672 | 464 |
| IntentSyncOk | 464 | 128 | 672 | 464 |
| IntentSyncFail | 928 | 256 | 1344 | 928 |
| PublishIntent | 464 | 128 | 672 | 464 |
| QueueOk | 432 | 128 | 608 | 432 |
| QueueFail | 32 | 0 | 64 | 32 |
| Retry | 370112 | 318480 | 808256 | 604128 |
| Conflict | 370112 | 318480 | 808256 | 604128 |
| RejectUnavailable | 365008 | 314016 | 798688 | 599120 |
| GateChange | 674136 | 616096 | 1401936 | 981624 |
| MarketChange | 674136 | 616096 | 1401936 | 981624 |
| SnapshotExpire | 337068 | 308048 | 700968 | 490812 |
| Expire | 671816 | 613856 | 1397248 | 979304 |
| PrepareFact | 240464 | 237216 | 595616 | 567280 |
| RejectPublicFact | 327264 | 290512 | 683136 | 543808 |
| RejectPoisonFact | 171968 | 163328 | 361088 | 262912 |
| FactSyncOk | 98672 | 96992 | 209440 | 144144 |
| FactSyncFail | 197344 | 193984 | 418880 | 288288 |
| ApplyFact | 98672 | 96992 | 209440 | 144144 |
| CheckAbort | 4720 | 2768 | 4096 | 4720 |
| RejectAbort | 7968 | 15744 | 17216 | 7968 |
| AbortSyncOk | 2592 | 1616 | 4096 | 2592 |
| AbortSyncFail | 5184 | 3232 | 8192 | 5184 |
| PublishAbort | 2592 | 1616 | 4096 | 2592 |
| PollStart | 19104 | 17136 | 38400 | 19104 |
| DriveBudgetError | 19104 | 17136 | 38400 | 19104 |
| DriveIdle | 11480 | 10368 | 23184 | 11480 |
| DriveReady | 1496 | 1472 | 2960 | 1496 |
| DriveFlush | 4736 | 4224 | 9856 | 4736 |
| DriveExpire | 6856 | 6192 | 14192 | 6856 |
| DriveFailure | 19104 | 17136 | 38400 | 19104 |
| DriveFlushThenClose | 2368 | 2112 | 4928 | 2368 |
| SettleNone | 13312 | 11968 | 25888 | 13312 |
| SettleLive | 27424 | 24768 | 56768 | 27424 |
| SettleFlushed | 4736 | 4224 | 9856 | 4736 |
| SettleNoBytes | 4488 | 4080 | 9264 | 4488 |
| SettleFailed | 9472 | 8448 | 19712 | 9472 |
| TryExpire | 6856 | 6192 | 14192 | 6856 |
| TryNotReady | 936 | 912 | 1872 | 936 |
| FirstGateDenied | 1034 | 976 | 2392 | 1034 |
| ExpireInsideGate | 168 | 82 | 72 | 168 |
| TryBlocked | 5576 | 4634 | 10216 | 5576 |
| FirstAccept | 168 | 82 | 72 | 168 |
| ContinueAccept | 2368 | 2112 | 4928 | 2368 |
| TryFailure | 2872 | 2358 | 5144 | 2872 |
| PollEnd | 39680 | 35360 | 79456 | 39680 |
| CancelZero | 4240 | 3936 | 8672 | 4240 |
| CancelAccepted | 9472 | 8448 | 19712 | 9472 |
| Crash | 834864 | 769472 | 1725024 | 1348272 |
| RecoverOk | 116224 | 58272 | 254752 | 338272 |
| RecoverInvalid | 27488 | 80096 | 56704 | 67744 |
| RecoverUnavailable | 143712 | 138368 | 311456 | 406016 |

20 项不变量通过，1576 行源码锚点零漂移。源码、最终模型、工具、配置和日志摘要见 `04_evidence/results.json`。

初模的强序号性质实际失败，继而通过普通与 MAX 序号 Rust 测试确认缺陷。修复前模型和轨迹保存在 `pre-metadata-review/`。首轮 v2 在新增成功前缀性质之前的结果保存在 `pre-success-prefix/`，没有与最终配置混用。早期参数拼写错误未启动 TLC，日志为 `v2-cli-error.log`。首次锚点检查指出 binding 注释越界，最终映射已按真实函数范围修正。

## 先前运行记录

| 运行 | generated | distinct | 深度 | 结果 |
|---|---:|---:|---:|---|
| 初始资金诊断投影 | 2801719 | 403896 | 50 | PASS |
| 恢复序号反例 | 22876 | 6008 | 11 | AccountPrefix 失败，保留完整轨迹 |
| 初次 v2 主配置 | 5972071 | 961560 | 50 | PASS |
| 新性质前 L0_AccountGateway_v2 | 5972071 | 961560 | 50 | PASS |
| 新性质前 L0_AccountGateway_v2_boundary | 5456397 | 892832 | 50 | PASS |
| 新性质前 L0_AccountGateway_v2_two | 12639561 | 2024848 | 50 | PASS |
| 新性质前 L0_AccountGateway_v2_crashes | 9600215 | 1793656 | 50 | PASS |

所有运行使用同一脚本，以模块路径、配置路径和 `--det --coverage` 调用。初始两次模型文本和配置见 `pre-metadata-review/`；首次 v2 文本为该目录的 `v2-before-final-anchors.tla.txt`，配置为 `v2-sequence3.cfg.txt`。新性质前的四份精确配置与模型保存在 `pre-success-prefix/`，历史文本以 `.txt` 留存，避免被当前配置发现脚本重复执行。
