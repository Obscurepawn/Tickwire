# 实际 TLC 检查

最终模型执行以下命令，所有配置均完整结束、队列为零。使用 TLC 2.19、单 worker；独立临时状态目录在完成后清理。

```sh
java -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L1_reduce_shadow-primary -config tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow.cfg tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow.tla
```

```sh
java -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L1_reduce_shadow-open -config tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow_open.cfg tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow.tla
```

```sh
mkdir -p /tmp/tickwire-account-java-l1-tight
java -Djava.io.tmpdir=/tmp/tickwire-account-java-l1-tight -Xmx2g -cp tla/tools/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -metadir /tmp/tickwire-account-locked-L1_reduce_shadow-tight -config tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow_tight.cfg tla/execution_models/account/L1_reduce_shadow/02_models/L1_ReduceShadow.tla
```

| 配置 | generated | distinct | 深度 | 时间 |
|---|---:|---:|---:|---|
| L1_ReduceShadow | 4367058 | 136551 | 16 | 25s |
| L1_ReduceShadow_open | 838661 | 24510 | 15 | 05s |
| L1_ReduceShadow_tight | 3176738 | 99367 | 16 | 14s |

下表取各日志最后一次 coverage 统计的 generated 列；distinct=0 而 generated>0 的重复调用仍有覆盖。

| 动作 | primary | open | tight |
|---|---:|---:|---:|
| Init | 1 | 1 | 1 |
| Admit | 52 | 10 | 4 |
| RejectAdmission | 1478 | 727 | 1526 |
| RetryAdmission | 271572 | 48283 | 197204 |
| ConflictAdmission | 271572 | 48283 | 197204 |
| Fill | 154752 | 13856 | 111024 |
| ReplayFill | 116820 | 34427 | 86180 |
| ConflictFill | 116820 | 34427 | 86180 |
| Fee | 204108 | 36351 | 148044 |
| ReplayFee | 203536 | 36166 | 147856 |
| ConflictFee | 203536 | 36166 | 147856 |
| Acknowledge | 271572 | 48283 | 197204 |
| Unknown | 271572 | 48283 | 197204 |
| Terminal | 543144 | 96566 | 394408 |
| ReplayFillUnknown | 116820 | 34427 | 86180 |
| ReplayFillTerminal | 233640 | 68854 | 172360 |
| Reconcile | 106160 | 21550 | 78192 |
| RejectReconcile | 242380 | 39660 | 175708 |
| Abort | 1532 | 142 | 716 |
| ReplayAbort | 3012 | 274 | 3012 |
| RejectAbort | 808724 | 150058 | 586564 |
| ExternalPosition | 87708 | 17361 | 62748 |
| RejectFact | 136547 | 24506 | 99363 |

模型、配置、源码、工具与日志 SHA256 在 `04_evidence/results.json`。TLC 解析草稿的排版修正未作为源码缺陷；正式结果只对应本目录最终模型。

最终重跑时两项并发 Java 进程的标准模块临时文件发生冲突，解析未启动状态检查；改用各自 `java.io.tmpdir` 后重跑通过。这是工具临时文件冲突，未分类为源码或模型性质反例。
