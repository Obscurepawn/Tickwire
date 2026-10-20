# TLC 运行记录

使用仓库固定 jar，SHA-256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。每组单 worker、2 GiB 堆、`-coverage 1`，独立 Java 临时目录和 metadir，结束后清理临时文件。四组均为完整有限状态穷举。

从仓库根目录执行：

```bash
repo="$PWD"
dir="$repo/tla/execution_models/gateway/L0_journaled_sender/02_models"
for cfg in primary three_intents larger_prefix two_crashes; do
  scratch="$(mktemp -d)"
  mkdir -p "$scratch/java" "$scratch/states"
  (cd "$dir" && java -Xmx2g -Djava.io.tmpdir="$scratch/java" \
    -XX:+UseParallelGC -cp "$repo/tla/tools/tla2tools.jar" tlc2.TLC \
    -workers 1 -coverage 1 -metadir "$scratch/states" \
    -config "$cfg.cfg" L0_JournaledSender.tla)
  rm -rf "$scratch"
done
```

| 配置 | 意图 / 报文单位 / 单次接受上限 / 崩溃上限 | 生成状态 | 不同状态 | 深度 | TLC 耗时 | 结果 |
|---|---|---:|---:|---:|---:|---|
| primary | 2 / 2 / 1 / 1 | 48603 | 7572 | 42 | 1 秒 | PASS |
| three_intents | 3 / 2 / 1 / 1 | 427536 | 57624 | 60 | 3 秒 | PASS |
| larger_prefix | 2 / 3 / 2 / 1 | 72859 | 11188 | 42 | 1 秒 | PASS |
| two_crashes | 2 / 2 / 1 / 2 | 52995 | 8610 | 42 | 1 秒 | PASS |

每组 41 个 Next 动作均 `generated > 0`，加上 Init 为 42 行顶层动作统计。解析只读取日志最后一个 coverage block。原始日志和逐动作计数位于 [04_evidence/results.json](04_evidence/results.json)。耗时为 TLC 自身报告，未用于性能结论。

最终源码摘要见 [source.sha256](04_evidence/source.sha256)，模型及配置摘要见 [model.sha256](04_evidence/model.sha256)。锚点由 skill 的 `anchors.sh` 在只含本级副本的临时 workspace 中生成和核对，未改写其他模型的锁文件。
