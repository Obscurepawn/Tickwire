# L0_CommitGate 运行记录

2026-10-20 实际运行 TLC 1.7.4，单 worker，开启 action coverage；本级两个配置都运行到队列为空，没有启用状态截断。表中的时间是 canonical runner 的整秒墙钟耗时，包含 JVM 启动；模型编写与审阅时间未计入。

| 配置 | generated | distinct | 最大深度 | runner 时间 | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| L0_CommitGate.cfg | 21,077 | 2,236 | 15 | 1s | PASS |
| L0_CommitGate_scaling.cfg | 103,216 | 9,076 | 15 | 2s | PASS |

精确命令（仓库根目录）：

```sh
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/submission/L0_commit_gate/02_models/L0_CommitGate.tla L0_CommitGate.cfg \
  --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/submission/L0_commit_gate/02_models/L0_CommitGate.tla L0_CommitGate_scaling.cfg \
  --det --coverage
```

可移植的全配置复验入口为 `./scripts/check-tla.sh`。没有发生 invariant violation，未生成反例；runner 已删除 TLC scratch。

逐动作 coverage 的第二个数字是 generated；只有第二个数字为零才表示从未触发。全部动作均有 generated；某些动作 distinct=0 是到达已存在状态。

| 动作 | primary distinct:generated | scaling distinct:generated |
| --- | ---: | ---: |
| Init | 1:1 | 1:1 |
| Reserve | 52:52 | 18:330 |
| PersistLocal | 52:656 | 54:2640 |
| PersistReplica | 52:1416 | 54:5940 |
| AcceptCommit | 78:3264 | 144:13860 |
| BeginSend | 78:78 | 333:495 |
| Ack | 117:624 | 609:3960 |
| Uncertain | 78:624 | 150:3960 |
| AbortUnsent | 221:3624 | 1827:13860 |
| NoBytes | 78:312 | 666:1980 |
| NoBytesRejected | 311:7072 | 682:42576 |
| ToggleGate | 1118:2236 | 4538:9076 |
| Freeze | 0:1118 | 0:4538 |

运行对应的文件 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `crates/tickwire-execution/src/lib.rs` | `edecde54dfab0b1b24dd1c0da386d6fe71dd936af7d4e11272b89a59f41e105d` |
| `tla/execution_models/submission/L0_commit_gate/02_models/L0_CommitGate.tla` | `4efaa0316f5bfe683aa149d73aa424652dccefa1d19c49fe5db71004e1a0d777` |
| `tla/execution_models/submission/L0_commit_gate/02_models/L0_CommitGate.cfg` | `29bad961172d874f0c4ef9340cf40391d148ff99ebcc8fcd5a4ef81f812166c5` |
| `tla/execution_models/submission/L0_commit_gate/02_models/L0_CommitGate_scaling.cfg` | `9237714b6a4a2b4a75a3253bd8c8f6ce31935f15b50daa4547c83e4edc699aae` |
