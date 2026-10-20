# L0 运行记录

最终模型 SHA256 为 `f1d8faeb257415b5bdac4f9a121e3e8e1cfd7777f8d4df0e418449bc84658a87`。四个配置从同一冻结副本运行；运行前后 7 个源码/测试文件哈希一致。所有配置检查 14 个不变量。

| 配置 | 场景 | generated | distinct | 深度 | 外部计时（秒） | 结果 |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| L0_NativeOwner.cfg | Native，读数 0..2，最多 5 次调用 | 6,267 | 3,021 | 7 | 2.360 | PASS |
| L0_NativeOwner_generic.cfg | Generic，读数 0..2，最多 5 次调用 | 17,278 | 2,431 | 7 | 1.895 | PASS |
| L0_NativeOwner_long.cfg | Native，读数 0..3，最多 7 次调用 | 49,767 | 18,714 | 9 | 3.304 | PASS |
| L0_NativeOwner_unsupported.cfg | 不支持的平台，只执行构造失败 | 2 | 2 | 2 | 1.576 | PASS |

外部计时包含 JVM 启动和 skill runner 清理，为本机实际记录，不是时钟性能测量。模型深度包含构造、move 和初态；MaxCalls 只限制所计数的采样调用。

## 命令与运行环境

```sh
python3 tla/execution_models/system_clock/L0_native_owner/04_evidence/run.py
```

该脚本复制一个模型和全部配置，逐个调用 `vibe-tla/scripts/run_tlc.sh --det --coverage`。每个配置使用独立 `java.io.tmpdir`、单 worker、2 GiB 最大堆；每份原始日志首行保留实际执行命令。运行使用 `/opt/homebrew/opt/openjdk/bin/java`，TLC 1.7.4，jar SHA256 为 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`。

最终结果和每份配置、源码、日志 SHA 位于 [results.json](04_evidence/results.json)。初轮 4 份原始日志保存在 [runs/initial](04_evidence/runs/initial/)，未用来替代最终结果；初轮 Generic 纯 wrapper 的覆盖被 TLC 合并，最终文本加入由 helper 已蕴含的结果分类条件，使四种输入各有具名入口。迁移关系和状态计数保持一致。

## 动作覆盖

```sh
python3 scripts/tlc_coverage.py --model L0_NativeOwner \
  --model-file tla/execution_models/system_clock/L0_native_owner/02_models/L0_NativeOwner.tla \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_generic.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_long.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_unsupported.log
```

Native 两个配置各有 Init + 14 个 Next 入口，Generic 有 Init + 6 个，Unsupported 有 Init + 1 个。跨配置并集为 Init + 20 个 Next，全部 generated 大于 0；每份配置自己的入口也全部执行。`ClientNativeSticky` 的 distinct 为 0、generated 大于 0：该真实调用到达已由其他动作发现的状态，不能据此判成未覆盖。

[coverage.json](04_evidence/coverage.json) 保存跨配置并集；results 同时保存各日志最后一个完整覆盖块的逐入口计数。该集合包括相等读数、分别回退、首次失败、失败后再次调用和移交后的错误传播。
