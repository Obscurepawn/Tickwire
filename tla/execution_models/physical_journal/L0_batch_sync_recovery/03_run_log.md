# L0 运行记录

TLC 使用固定 1.7.4 release jar。

Jar SHA-256：`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。固定单 worker，三组 cfg 全部穷举完成；没有状态约束剪枝或超时截断。

| cfg | generated | distinct | depth | runner elapsed | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| primary | 2,423 | 1,816 | 18 | 1 s | PASS |
| batch | 3,518 | 2,695 | 20 | 1 s | PASS |
| scaling | 61,476 | 45,883 | 28 | 3 s | PASS |

每次运行使用独立 TLC metadir 和 Java 临时目录。实际命令通过 canonical runner：

```python
from pathlib import Path
import os, subprocess, tempfile
level = Path("tla/execution_models/physical_journal/L0_batch_sync_recovery")
runner = Path.home() / ".codex/skills/vibe-tla/scripts/run_tlc.sh"
for name, suffix in [("primary", ""), ("batch", "_batch"), ("scaling", "_scaling")]:
    with tempfile.TemporaryDirectory(prefix="tickwire-wal-tlc-") as scratch:
        env = dict(os.environ, JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=" + scratch)
        command = ["bash", str(runner), str(level / "02_models/L0_PhysicalJournal.tla"),
                   "L0_PhysicalJournal" + suffix + ".cfg", "--det", "--coverage",
                   "--", "-metadir", scratch + "/states"]
        with open(level / ("04_evidence/" + name + ".log"), "w") as output:
            subprocess.run(command, env=env, stdout=output, stderr=subprocess.STDOUT, check=True)
```

初次 SANY 因 `=<<` 被解析为 `=<` 而报错，改成 `= <<` 后通过，原始记录保留在 `parse-initial.log`。之后三组先跑通，修正一处源码注释范围后再完整复跑；表中和最终日志均对应当前模型文本。暂存 anchors 的首个临时目录未匹配工具目录规则，未生成锁；改成 canonical `execution_models/physical_journal/L0...` 路径后重新 update/check。

## 动作覆盖

计数使用 TLC 的 generated 后继数；不同状态为零但生成后继的幂等动作不算缺覆盖。每 cfg 的 16 个 Next 动作全部触发。

| action | primary | batch | scaling |
| --- | ---: | ---: | ---: |
| BeginAppend | 67 | 87 | 1097 |
| RejectInvalid | 170 | 217 | 2858 |
| WriteFrame | 117 | 172 | 1737 |
| WriteFailure | 117 | 172 | 1737 |
| SyncFailure | 39 | 43 | 579 |
| SyncSuccess | 39 | 43 | 579 |
| ReturnReceipt | 39 | 43 | 579 |
| RejectPoisoned | 294 | 396 | 4789 |
| BeginRecovery | 494 | 772 | 15282 |
| RecoveryReject | 317 | 538 | 10507 |
| RepairTail | 138 | 181 | 4183 |
| RecoverySyncFailure | 138 | 181 | 2503 |
| RecoverySyncSuccess | 138 | 181 | 2503 |
| Crash | 111 | 161 | 6066 |
| CorruptFrame | 153 | 264 | 4857 |
| LoseWholeTail | 51 | 66 | 1619 |

TLC 对依赖量词打印 `Next (115...)`、`Next (116...)`、`Next (117...)`，分别是 Crash、CorruptFrame、LoseWholeTail。其余名称由原日志直接提取。原日志、结构化结果和源码/model/cfg/jar 哈希保存在 `04_evidence/`。
