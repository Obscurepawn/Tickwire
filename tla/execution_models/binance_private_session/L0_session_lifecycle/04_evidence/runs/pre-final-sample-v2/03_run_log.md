# L0 执行记录

使用仓库固定 TLC jar（SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`）、Homebrew Java 24.0.2、单 worker、2 GiB 堆。每个运行使用独立 `java.io.tmpdir` 和 metadir，结束删除 scratch。实际命令完整保存在每份日志第一行。

```sh
/opt/homebrew/opt/openjdk/bin/java -Xmx2g \
  -Djava.io.tmpdir="$run_tmp/java" -XX:+UseParallelGC \
  -cp "$repo/tla/tools/tla2tools.jar" tlc2.TLC \
  -workers 1 -coverage 1 -metadir "$run_tmp/states" \
  -config L0_PrivateSession.cfg L0_PrivateSession.tla
```

命令在本级 `02_models` 中运行；其他配置替换 `-config` 参数。`run_tmp` 是先创建的独立临时目录，包含 java/states 子目录。

| 配置 | generated | distinct | depth | TLC 时间 | 结果 |
| --- | ---: | ---: | ---: | --- | --- |
| L0_PrivateSession | 20,307 | 9,675 | 24 | 1 s | PASS |
| L0_PrivateSession_logon | 3,113 | 1,653 | 24 | 0 s（日志精度） | PASS |
| L0_PrivateSession_usdm | 495,148 | 242,485 | 30 | 13 s | PASS |
| L0_PrivateSession_buffer | 2,077 | 939 | 18 | 0 s（日志精度） | PASS |

最终每份日志末轮 coverage 包含 Init 与 28 个具名 Next 入口，全部 generated 大于0。Spot 的 OldReply/ReplyEOF 与 USD-M 的 Renew/LeaseExpires 分别通过专用 Next 展开，避免用产品常量隐藏实际动作覆盖。检查15项不变量，无反例。

开发过程的初始解析错误和中间运行保留在 `04_evidence/initial*.log`。初稿未展开条件Next导致仅显示聚合覆盖，后来配置直接指定 SpotNext/UsdmNext；独立审阅又修正 SinkFailure 的永久停止行为，修前运行归 `04_evidence/runs/pre-sink-review`。这些中间输出不作为当前模型的证据；当前模型、配置、源码、锁及最终日志的哈希统一见 `04_evidence/results.json`。
