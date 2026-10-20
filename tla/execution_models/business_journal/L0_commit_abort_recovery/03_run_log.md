# L0 实际运行记录

固定 TLC 1.7.4、单 worker，启用动作覆盖。每组配置从真实模型运行，退出码均为 0。

```bash
cd tla/execution_models/business_journal/L0_commit_abort_recovery/02_models
java -Djava.io.tmpdir="$TMP" -XX:+UseParallelGC -cp "$REPO/tla/tools/tla2tools.jar" tlc2.TLC \
  -workers 1 -coverage 1 -metadir "$TMP/states" -config "$CFG" L0_BusinessJournal.tla
```

`$CFG` 依次为下表文件；每次运行使用独立临时目录，结束清理。

| 配置 | generated | distinct | depth | 秒 |
| --- | ---: | ---: | ---: | ---: |
| `L0_BusinessJournal.cfg` | 130 | 72 | 14 | 1.226 |
| `L0_BusinessJournal_competing.cfg` | 571 | 345 | 26 | 1.404 |
| `L0_BusinessJournal_scaling.cfg` | 1747 | 879 | 28 | 1.301 |

每组都覆盖 23 个迁移动作与 Init，generated 均大于 0。原始 TLC 输出、每动作计数和运行摘要在 `04_evidence`。本层没有不变量反例。
