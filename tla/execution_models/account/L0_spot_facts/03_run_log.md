# 实际 TLC 检查

最终模型执行以下命令，所有配置均完整结束、队列为零。使用 TLC 2.19、单 worker；独立临时状态目录在完成后清理。

```sh
java -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L0_spot_facts-primary -config tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3.cfg tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3.tla
```

```sh
mkdir -p /tmp/tickwire-account-java-l0-two
java -Djava.io.tmpdir=/tmp/tickwire-account-java-l0-two -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L0_spot_facts-two -config tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3_two.cfg tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3.tla
```

```sh
java -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L0_spot_facts-tight -config tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3_tight.cfg tla/execution_models/account/L0_spot_facts/02_models/L0_SpotFacts_v3.tla
```

| 配置 | generated | distinct | 深度 | 时间 |
|---|---:|---:|---:|---|
| L0_SpotFacts_v3 | 7892392 | 176596 | 16 | 40s |
| L0_SpotFacts_v3_two | 8626384 | 192984 | 15 | 36s |
| L0_SpotFacts_v3_tight | 8053279 | 180131 | 16 | 41s |

下表取各日志最后一次 coverage 统计的 generated 列；distinct=0 而 generated>0 的重复调用仍有覆盖。

| 动作 | primary | two | tight |
|---|---:|---:|---:|
| Init | 1 | 1 | 1 |
| Admit | 8 | 264 | 50 |
| RejectAdmission | 2756 | 868 | 2130 |
| RetryAdmission | 352092 | 384868 | 359218 |
| ConflictAdmission | 352092 | 384868 | 359218 |
| Fill | 547696 | 596880 | 557280 |
| ReplayFill | 430336 | 471296 | 439796 |
| ConflictFill | 430336 | 471296 | 439796 |
| Fee | 268836 | 294204 | 275976 |
| ReplayFee | 262480 | 286800 | 267226 |
| ConflictFee | 262480 | 286800 | 267226 |
| Unknown | 176954 | 209726 | 184080 |
| Acknowledge | 352092 | 384868 | 359218 |
| Terminal | 1056276 | 1154604 | 1077654 |
| ReplayFillUnknown | 430336 | 471296 | 439796 |
| ReplayFillTerminal | 1291008 | 1413888 | 1319388 |
| Reconcile | 60616 | 69576 | 62160 |
| RejectReconcile | 299624 | 324728 | 303576 |
| Abort | 842 | 1102 | 884 |
| ReplayAbort | 1940 | 1940 | 1884 |
| RejectAbort | 1136996 | 1223528 | 1156592 |
| RejectFact | 176595 | 192983 | 180130 |

模型、配置、源码、工具与日志 SHA256 在 `04_evidence/results.json`。TLC 解析草稿的排版修正未作为源码缺陷；正式结果只对应本目录最终模型。

最终重跑时两项并发 Java 进程的标准模块临时文件发生冲突，解析未启动状态检查；改用各自 `java.io.tmpdir` 后重跑通过。这是工具临时文件冲突，未分类为源码或模型性质反例。
