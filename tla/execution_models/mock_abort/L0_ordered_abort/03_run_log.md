# L0 实际运行

在仓库根目录执行（每个 cfg 使用同一份 module）：

```sh
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/mock_abort/L0_ordered_abort/02_models/L0_OrderedAbort.tla --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/mock_abort/L0_ordered_abort/02_models/L0_OrderedAbort.tla L0_OrderedAbort_boundary.cfg --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/mock_abort/L0_ordered_abort/02_models/L0_OrderedAbort.tla L0_OrderedAbort_scaling.cfg --det --coverage
```

TLC release 1.7.4（程序显示 2.19），Java 1.8.0_462，单 worker，`-coverage 1`。jar SHA-256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。机器没有 timeout/gtimeout，脚本的 1800 秒配置不产生实际超时包装；本轮运行均自然穷举结束。

| cfg | 订单 / 重启 | 生成状态 | 不同状态 | 深度 | 脚本耗时 | 结果 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| primary | 2 / 1 | 3,013 | 2,051 | 26 | 2 秒 | PASS |
| boundary | 2 / 1 | 637 | 457 | 22 | 1 秒 | PASS |
| scaling | 3 / 2 | 158,415 | 102,149 | 40 | 4 秒 | PASS |

原始日志位于 [primary](runs/primary.log)、[boundary](runs/boundary.log)、[scaling](runs/scaling.log)，机器可读结果为 [results.json](runs/results.json)。下表为 generated 计数；Init 各 1，15 个 Next 动作在每组都实际触发。

| 动作 | primary | boundary | scaling |
| --- | ---: | ---: | ---: |
| Reserve | 14 | 6 | 315 |
| Retry | 76 | 12 | 1,470 |
| CommitFail | 42 | 12 | 780 |
| PersistIntent | 42 | 12 | 780 |
| LoseReceipt | 28 | 6 | 549 |
| AcceptReceipt | 28 | 6 | 549 |
| BeginSend | 28 | 6 | 549 |
| Dispatch | 112 | 24 | 2,196 |
| StartTerminal | 296 | 36 | 8,676 |
| FactFail | 204 | 42 | 5,445 |
| PersistRelease | 204 | 42 | 5,445 |
| ApplyRelease | 204 | 42 | 5,445 |
| Crash | 645 | 145 | 50,280 |
| ReplayOne | 794 | 178 | 62,238 |
| FinishRecovery | 295 | 67 | 13,697 |

首次运行因 `frozen'` 赋值右侧缺少括号，TLC 报 successor 未完整赋值，退出 75。该作者错误及前缀 trace 保存在 [authoring-incomplete-state.log](runs/authoring-incomplete-state.log)；修复后重新执行三组正式配置，没有将它报告为源码反例。
