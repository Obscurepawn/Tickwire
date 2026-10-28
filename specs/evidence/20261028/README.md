# 2026-10-28 阶段证据

本目录按源码快照保存验证结果。每份报告只覆盖其引用的源码清单与二进制摘要；后续改动保留较早记录。

| 阶段（按执行顺序） | 报告 | 范围 |
|---|---|---|
| 标的索引、连续盘口、恢复修复 | [summary.json](summary.json) | 五轮独立索引基准；普通及 16 MiB＋1 字节策略 ELF 的 schema3 进程交接；有界输入采集库 |
| 完整外部 ID 与本地盘口 key 接线 | [dense-book.json](dense-book.json) | 完整 `u64` 经真实 SBE 网络与共享内存发布；高位 ID 与本地槽隔离；重新执行全仓与进程验收 |
| 发布热路径 | [publisher-hot.json](publisher-hot.json) | 共享 JSON/SBE 解码工作区、真实发布零分配、2 MiB 栈、缓存预算与失败边界；再次执行全仓与进程验收 |

`summary.json` 中的 `final` / `verified` 文件名指第一阶段结束时的快照。`dense-book.json` 记录的 JSON 栈溢出和 8 MiB 测试绕行属于第二阶段，已在发布热路径阶段修复；对应旧报告保持原样。

最新阶段的 [源码清单](publisher-hot-source-sha256.json) 与 [二进制清单](publisher-hot-binaries.json) 可用于核对文件 SHA-256。报告中的 `/tmp/` 路径记录本次运行位置，原始日志摘要用于校验保留的本地输出。全链路使用本机 TLS 交易所 fixture；耗时是场景运行时间，基准 smoke 不作为性能数据。本轮没有新增 TLC 结果。
