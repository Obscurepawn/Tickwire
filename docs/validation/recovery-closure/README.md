# 恢复依赖验证记录

当前 12 项 Rust 测试在 macOS 和 Linux aarch64 通过，Clippy 使用 `-D warnings`。验证包括内存 Mock 的故障注入和真实文件后端的锁、重开及损坏检测。Linux 使用 `scripts/check-linux.sh` 固定的 Rust 1.99.1 容器。

| 记录 | 实际内容 |
| --- | --- |
| [初始失败](red.log) | API 可编译；5 项测试中 4 项因功能未实现而断言失败 |
| [重新同步失败](revalidation-red.log) | 旧 `verify` 读到正确对象后直接成功，没有重新确认同步；新增测试断言失败 |
| [最终通过](green.log) | 12 项 Rust 测试通过 |
| [静态检查](clippy.log) | Clippy 通过 |
| [Linux 验证](linux.log) | 同版源码的 12 项测试及 Clippy 通过 |
| [独立样本生成器](generate-golden.py) | Python 标准库独立生成 1010 字节 manifest 和 158 字节输入块；`--check` 与 Rust 测试中的固定样本比较 |
| [源码审阅](review.md) | 修正父目录同步重试和相对工作目录问题；注明源码推导与实际故障注入的区别 |

`partial_binding_publication_retries_sync_before_receipt` 模拟绑定已写入、目录同步却返回错误的后端。再次验证仍遇到同步错误时必须失败，后续全部同步成功才返回结果。该用例覆盖 3 次各 14 项写入尝试，共 42 次。真实文件测试独立检查独占锁、目录重开、不可变绑定，以及对象损坏后验证失败且不覆盖损坏内容。

编码测试逐一拒绝 manifest 的 1010 个截断前缀，并检查非最短编码、不定长结构和尾随字节。输入测试对摘要重新计算后仍然格式错误的内容进行检查，涵盖 157 个非空截断前缀、非法输入类型、缺口、重叠及决策边界错误。对象 hash 正确不会绕过内容校验。

模型运行及独立审阅位于 [L0_publish_verify](../../../tla/execution_models/recovery_closure/L0_publish_verify/03_run_log.md)。首版模型遗漏了多个未完成目录项分别保留或丢失的情况；修正后重新执行三组配置。首版记录留在 `pre-review/`，作为建模范围修正的历史记录，没有将该问题报告为 Rust 实现缺陷。

复跑命令在仓库根目录执行：

```sh
cargo test -p tickwire-recovery
cargo fmt -p tickwire-recovery --check
cargo clippy -p tickwire-recovery --all-targets -- -D warnings
python3 docs/validation/recovery-closure/generate-golden.py --check
sh docs/validation/recovery-closure/run-models.sh
```

kind 1–11 的业务授权与内容 schema 由可信授权服务验证，模型也未模拟真实策略输入采集、字节解析、文件系统内部实现或跨主机复制。相关业务接入由 `tickwire-business-journal` 单独验证。
