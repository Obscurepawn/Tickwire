# L1_coverage 运行记录

工具包为官方 release 1.7.4，TLC 启动横幅为 2.19；jar SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。本机 Java 1.8.0_462、macOS 15.7；每组 `-workers 1 -coverage 1`，穷举结束，队列均为 0。运行时长是本次工具墙上时间，不是盘口性能指标。

从仓库根目录逐组执行（`cfg` 取下表名称）：

```bash
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/book_state/L1_coverage/02_models/L1_BookCoverage.tla \
  "$cfg.cfg" --det --coverage
```

仓库自带 `scripts/check-tla.sh` 也会运行这些配置，使用相同固定 jar、单 worker 与覆盖检查。

| 配置 | generated | distinct | depth | 秒 | 结果 |
|---|---:|---:|---:|---:|---|
| [L1_BookCoverage](runs/L1_BookCoverage.log) | 45,523 | 1,471 | 6 | 3 | PASS |
| [L1_Ask](runs/L1_Ask.log) | 45,523 | 1,471 | 6 | 1 | PASS |
| [L1_AskScaling](runs/L1_AskScaling.log) | 585,796 | 8,244 | 6 | 4 | PASS |
| [L1_BidScaling](runs/L1_BidScaling.log) | 519,202 | 7,050 | 6 | 6 | PASS |

共 15 个 Next 动作，全部配置 generated>0。下表为 generated；原始日志同时保留 distinct:generated，允许仅生成已有状态的动作 distinct=0。Init 单独记为 1。

| 动作 | L1_BookCoverage | L1_Ask | L1_AskScaling | L1_BidScaling |
|---|---:|---:|---:|---:|
| SnapshotAccepted | 13865 | 13865 | 223344 | 129822 |
| SnapshotRejected | 235 | 235 | 1269 | 843 |
| ApplyAccepted | 22194 | 22194 | 250344 | 333630 |
| CapacityRejected | 2106 | 2106 | 67800 | 14589 |
| Invalidate | 1471 | 1471 | 8244 | 7050 |
| IgnoreStale | 900 | 900 | 4971 | 4299 |
| RejectIdentity | 1471 | 1471 | 8244 | 7050 |
| Rebuild | 725 | 725 | 8166 | 7005 |
| ObserveKnownBest | 288 | 288 | 1662 | 1629 |
| ObserveHiddenBest | 190 | 190 | 1176 | 975 |
| ObserveUnknownZero | 446 | 446 | 1797 | 3180 |
| ObserveKnownZero | 462 | 462 | 1698 | 2649 |
| ObserveKnownBeyondCoverage | 380 | 380 | 2688 | 2970 |
| ObserveCompleteEmpty | 10 | 10 | 15 | 15 |
| ObserveNotLive | 779 | 779 | 4377 | 3495 |

结构化计数、模块/配置/日志摘要见 [results.json](runs/results.json)。
