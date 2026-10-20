# 当前 L0 实际运行

三组原配置保持 Roots、Dependencies、MaxCalls、MaxCrashes 不变，扩大故障选择后全部重新执行。TLC release 1.7.4（程序显示 2.19），Java 1.8.0_462，单 worker，`-coverage 1`。jar SHA-256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。

在仓库根目录执行：

```sh
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/recovery_closure/L0_publish_verify/02_models/L0_RecoveryClosure.tla --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/recovery_closure/L0_publish_verify/02_models/L0_RecoveryClosure.tla L0_Conflict.cfg --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh tla/execution_models/recovery_closure/L0_publish_verify/02_models/L0_RecoveryClosure.tla L0_MultipleDependencies.cfg --det --coverage
```

| 配置 | 生成状态 | 不同状态 | 深度 | 脚本耗时 | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| L0_RecoveryClosure | 5,093 | 2,345 | 51 | 1 秒 | PASS |
| L0_Conflict | 37,333 | 15,097 | 56 | 2 秒 | PASS |
| L0_MultipleDependencies | 9,456 | 3,898 | 66 | 1 秒 | PASS |

## 动作覆盖

下表为 generated 计数。23 个覆盖入口包括 Init 和 22 个 Next 动作，每组全部触发。IoFailure/Crash 的选择量移到动作内部，以保留明确的动作名；这不改变状态选择。机器可读结果见 [results.json](runs/results.json)。

| 动作 | 主配置 | 两个 root 竞争 | 两个依赖 |
| --- | ---: | ---: | ---: |
| Init | 1 | 1 | 1 |
| Open | 224 | 1269 | 318 |
| RootSync | 222 | 1267 | 316 |
| ParentSync | 222 | 1267 | 316 |
| OpenFailure | 444 | 2534 | 632 |
| Seed | 239 | 2276 | 800 |
| BeginPublish | 478 | 4552 | 800 |
| BeginVerify | 239 | 2276 | 400 |
| RejectShape | 239 | 1138 | 400 |
| Lookup | 62 | 516 | 100 |
| CheckObject | 146 | 1124 | 242 |
| CheckInputs | 44 | 160 | 24 |
| PrepareBinding | 22 | 80 | 12 |
| Put | 240 | 1524 | 446 |
| TempWrite | 26 | 408 | 112 |
| FileSyncNew | 26 | 408 | 112 |
| Rename | 26 | 408 | 112 |
| FileSyncExisting | 214 | 1108 | 334 |
| DirSync | 230 | 1390 | 386 |
| IoFailure | 596 | 4792 | 1306 |
| ReturnPut | 164 | 778 | 220 |
| Deliver | 18 | 60 | 10 |
| Crash | 971 | 7997 | 2057 |

原始日志：[主配置](runs/L0_RecoveryClosure.log)、[竞争配置](runs/L0_Conflict.log)、[多依赖配置](runs/L0_MultipleDependencies.log)。首次模型编写错误日志和三份修正前正式日志原样保存在 [pre-review](pre-review/README.md)，旧故障域的结果不替代本次结果。脚本完成后已清理 TLC scratch。
