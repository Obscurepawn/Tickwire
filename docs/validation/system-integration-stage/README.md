# 系统运行组合阶段验证

固定源码 `final-system-check-20261020T204555Z` 在 macOS arm64 和 Linux arm64 完成全工作区测试、文档测试与 Clippy，所有命令退出码均为 0。格式、设计契约、OpenAPI 和文档检查通过。[结果](results.json)保存命令与统计，[源码清单](source-manifest.json)覆盖 473 个构建输入；归档时逐项确认工作区与测试源码相同。

| 检查 | macOS | Linux |
|---|---:|---:|
| all-targets 日志汇总 | 1,090 通过，0 失败，4 ignored | 1,165 通过，0 失败，5 ignored |
| 文档测试 | 1 通过 | 1 通过 |
| Clippy，warnings 作为错误 | 通过 | 通过 |

数字按日志中的 `test result` 行求和，包含测试所启动的子进程 harness 输出，未去重为唯一测试数。完整输出分别保存在 [macOS 测试](native-tests.log)、[Linux 测试](linux-tests.log)和两平台的 doctests/clippy 日志中。性能用例仅使用 1,000 个样本完成回归检查，本轮没有形成新的性能结论。

同一冻结源码的 [schema2 Spot 多进程交接](../core-chain/spot-handover-v2/README.md)通过：旧进程正常退出，真实迁移和独立回滚探测完成，新实例持续消费至账户位置 9；迟到成交和费用只入账一次，9 条源账户记录与镜像逐字节相同。SBE、收入基线导入、配置权限与三 voter 持久共识的针对性失败回归仍保留在各自目录。

本阶段完成的是当前已实现模块及其运行组合。通用策略输入尾部回放、配置控制服务与物化、完整收入闭合、更多订单和账户行为、故障隔离及自动接管继续按[剩余代码清单](../../implementation/remaining-code-audit.md)实现。全部系统代码完成后再执行新一轮模块及全局 TLA+。

`metadata.json` 记录冻结时的基线 commit 和路径；`artifacts.sha256` 校验直接归档的原始日志与清单。`documentation.log` 对应冻结源码，`live-documentation.log` 对应随后加入接口和运行证据后的文档检查。
