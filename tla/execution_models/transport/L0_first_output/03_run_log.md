# 运行记录

使用仓库固定的 TLC 1.7.4，jar SHA-256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。单 worker，最大堆 2 GiB，`-coverage 1`。每次运行使用独立 java 临时目录和 metadir，结束后删除 scratch。

从仓库根目录执行：

```bash
repo="$PWD"
dir="$repo/tla/execution_models/transport/L0_first_output/02_models"
for cfg in primary two_sends larger_buffer plain; do
  scratch="$(mktemp -d)"
  mkdir -p "$scratch/java" "$scratch/states"
  (cd "$dir" && java -Xmx2g -Djava.io.tmpdir="$scratch/java" \
    -XX:+UseParallelGC -cp "$repo/tla/tools/tla2tools.jar" tlc2.TLC \
    -workers 1 -coverage 1 -metadir "$scratch/states" \
    -config "$cfg.cfg" L0_FirstOutput.tla)
  rm -rf "$scratch"
done
```

| 配置 | 生成状态 | 不同状态 | 深度 | Next 动作数 | 结果 |
|---|---:|---:|---:|---:|---|
| primary | 2733 | 688 | 14 | 19 | PASS |
| two_sends | 14793 | 3928 | 20 | 19 | PASS |
| larger_buffer | 21975 | 5944 | 20 | 19 | PASS |
| plain | 10183 | 2832 | 13 | 16 | PASS |

TLC 报告每组耗时 0–1 秒。四组的所有 Next 动作均有 `generated > 0`；统计行另含 Init，所以 TLS 日志共 20 条动作统计，普通 TCP 共 17 条。原始日志与机器读取结果在 [04_evidence](04_evidence/results.json)。
