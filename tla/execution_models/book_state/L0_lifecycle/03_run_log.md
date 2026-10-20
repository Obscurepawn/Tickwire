# L0_lifecycle 运行记录

工具包为官方 release 1.7.4，TLC 启动横幅为 2.19；jar SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。本机 Java 1.8.0_462、macOS 15.7；每组 `-workers 1 -coverage 1`，穷举结束，队列均为 0。运行时长是本次工具墙上时间，不是盘口性能指标。

从仓库根目录逐组执行（`cfg` 取下表名称）：

```bash
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/book_state/L0_lifecycle/02_models/L0_BookState.tla \
  "$cfg.cfg" --det --coverage
```

仓库自带 `scripts/check-tla.sh` 也会运行这些配置，使用相同固定 jar、单 worker 与覆盖检查。

| 配置 | generated | distinct | depth | 秒 | 结果 |
|---|---:|---:|---:|---:|---|
| [L0_BookState](runs/L0_BookState.log) | 111,457 | 1,392 | 6 | 2 | PASS |
| [L0_SpotScaling](runs/L0_SpotScaling.log) | 3,042,646 | 8,499 | 6 | 12 | PASS |
| [L0_UsdM](runs/L0_UsdM.log) | 115,809 | 1,880 | 6 | 2 | PASS |
| [L0_UsdMScaling](runs/L0_UsdMScaling.log) | 2,220,496 | 10,524 | 6 | 14 | PASS |

共 19 个 Next 动作，全部配置 generated>0。下表为 generated；原始日志同时保留 distinct:generated，允许仅生成已有状态的动作 distinct=0。Init 单独记为 1。

| 动作 | L0_BookState | L0_SpotScaling | L0_UsdM | L0_UsdMScaling |
|---|---:|---:|---:|---:|
| SnapshotAccepted | 2184 | 27972 | 2088 | 27000 |
| SnapshotRejected | 182 | 777 | 174 | 750 |
| SnapshotWrongIdentity | 1392 | 8499 | 1880 | 10524 |
| SnapshotBadState | 1210 | 7722 | 1706 | 9774 |
| ApplyAccepted | 60288 | 2327940 | 53696 | 1519236 |
| ApplyStale | 10592 | 121500 | 10176 | 112050 |
| ApplyBadSequence | 7808 | 129060 | 17176 | 239382 |
| ApplyBadClock | 2688 | 78300 | 1792 | 30780 |
| ApplyBadPayload | 15072 | 258660 | 13424 | 168804 |
| ApplyWrongIdentity | 1392 | 8499 | 1880 | 10524 |
| ApplyBadState | 760 | 4503 | 1024 | 5691 |
| RebuildAccepted | 696 | 8499 | 940 | 10524 |
| RebuildRejected | 2088 | 16998 | 2820 | 21048 |
| InvalidateAccepted | 1392 | 8499 | 1880 | 10524 |
| InvalidateWrongGeneration | 1392 | 16998 | 1880 | 21048 |
| ViewAccepted | 784 | 5751 | 1176 | 4104 |
| ViewNotLive | 928 | 5259 | 1184 | 6420 |
| ViewClockRegressed | 448 | 4698 | 672 | 6156 |
| ViewStale | 160 | 2511 | 240 | 6156 |

结构化计数、模块/配置/日志摘要见 [results.json](runs/results.json)。
