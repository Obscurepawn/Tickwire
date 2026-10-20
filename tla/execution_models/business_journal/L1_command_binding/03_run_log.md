# L1 运行记录

使用仓库固定的 TLC 1.7.4，单 worker、action coverage，实际执行三组配置：

```sh
/Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/business_journal/L1_command_binding/02_models/L1_CommandBinding.tla \
  L1_CommandBinding.cfg --det --coverage
```

另两组将 cfg 替换为 `L1_CommandBinding_competing.cfg` 和 `L1_CommandBinding_recovery.cfg`。从仓库也可用 `scripts/check-tla.sh` 执行全部已提交配置。

| 配置 | Orders / Variants / Capital / MaxCrashes | generated | distinct | depth | 秒 |
|---|---|---:|---:|---:|---:|
| primary | 1 / 2 / 1 / 1 | 670 | 376 | 15 | 1 |
| competing | 2 / 2 / 1 / 1 | 4491 | 2664 | 27 | 2 |
| recovery | 2 / 2 / 2 / 2 | 14799 | 7326 | 29 | 1 |

三组全部通过 10 个不变量，每组 Init 与全部 29 个 Next 动作的 generated 均大于零。日志保存在 `runs/primary.log`、`runs/competing.log`、`runs/recovery.log`。
