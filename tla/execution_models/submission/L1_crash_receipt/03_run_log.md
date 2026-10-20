# L1_CrashReceipt 运行记录

2026-10-20 实际运行 TLC 1.7.4，单 worker，开启 action coverage；本级三个配置都运行到队列为空，没有启用状态截断。表中的时间是 canonical runner 的整秒墙钟耗时，包含 JVM 启动；模型编写与审阅时间未计入。

| 配置 | generated | distinct | 最大深度 | runner 时间 | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| L1_CrashReceipt.cfg | 6,539 | 1,744 | 14 | 1s | PASS |
| L1_CrashReceipt_scaling.cfg | 417,807 | 96,828 | 22 | 5s | PASS |
| L1_CrashReceipt_pending.cfg | 833,051 | 186,148 | 22 | 7s | PASS |

精确命令（仓库根目录）：

```sh
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt.tla L1_CrashReceipt.cfg \
  --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt.tla L1_CrashReceipt_scaling.cfg \
  --det --coverage
bash /Users/bytedance/.codex/skills/vibe-tla/scripts/run_tlc.sh \
  tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt.tla L1_CrashReceipt_pending.cfg \
  --det --coverage
```

可移植的全配置复验入口为 `./scripts/check-tla.sh`。没有发生 invariant violation，未生成反例；runner 已删除 TLC scratch。

逐动作 coverage 的第二个数字是 generated；只有第二个数字为零才表示从未触发。全部动作均有 generated；某些动作 distinct=0 是到达已存在状态。

| 动作 | primary distinct:generated | scaling distinct:generated | pending distinct:generated |
| --- | ---: | ---: | ---: |
| Init | 1:1 | 1:1 | 1:1 |
| StartSubmit | 4:4 | 168:328 | 368:728 |
| StoreDependency | 4:4 | 168:168 | 368:368 |
| DependencyFailure | 4:4 | 128:168 | 176:368 |
| Resume | 500:1008 | 35408:71152 | 71400:143536 |
| CommitAndDeliver | 440:944 | 26952:65424 | 53464:131248 |
| CommitAndLoseReceipt | 89:944 | 2621:65424 | 5758:131248 |
| CommitFailure | 4:504 | 217:35576 | 390:71768 |
| AcceptReceipt | 136:168 | 8176:10176 | 16176:20400 |
| RejectReceipt | 0:272 | 0:16776 | 0:33064 |
| BeginSend | 4:4 | 120:120 | 504:504 |
| RejectBegin | 12:132 | 544:8056 | 1448:15672 |
| Dispatch | 16:16 | 480:480 | 2016:2016 |
| Respond | 12:16 | 336:480 | 838:2016 |
| AbortUnsent | 20:280 | 744:22160 | 3324:45248 |
| Uncertain | 20:224 | 648:13416 | 2152:26520 |
| ToggleGate | 188:520 | 6648:19412 | 14098:37048 |
| Freeze | 132:182 | 4598:6218 | 7450:14242 |
| Crash | 120:1192 | 5944:76328 | 4804:152252 |
| Restart | 38:120 | 2927:5944 | 1413:4804 |

运行对应的文件 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `crates/tickwire-execution/src/lib.rs` | `edecde54dfab0b1b24dd1c0da386d6fe71dd936af7d4e11272b89a59f41e105d` |
| `crates/tickwire-execution/src/ports.rs` | `5c8b225fef70d58d50f21e70cf12a3e802e5bf97ad7496a802a88c2cd1a92d46` |
| `crates/tickwire-mock/src/lib.rs` | `bacb2f5da317492cab0734d44797e85ef502f07443ca39b4b76e71c1b0c347c5` |
| `tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt.tla` | `0be1b5dfe2ae093f824a418f895961d17e7a1b91395216a364ed51679ed0a687` |
| `tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt.cfg` | `af17026ec1b022518a2ba8b1c0185bb3945c37d56985d865bdbc79a2c85a60a7` |
| `tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt_scaling.cfg` | `6bc54864737f1414d15a53bc69f5c0b41785fc2c1eea056107411b2b5c7d1458` |
| `tla/execution_models/submission/L1_crash_receipt/02_models/L1_CrashReceipt_pending.cfg` | `dbdf5b49a47cdf0571fdfb12958e0ae66a4a1fa84299426148f67e370dfdee09` |
